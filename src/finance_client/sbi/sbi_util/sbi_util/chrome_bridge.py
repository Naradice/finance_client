"""Adapt permission-based Chrome CDP to ChromeDriver's HTTP discovery protocol.

The upstream connection is approved by Chrome's user prompt. No browser launch,
profile copying or broker API is involved. The bridge lives only for the driver
session and never forwards Browser.close to a borrowed browser.
"""

import asyncio
from concurrent.futures import Future
import json
import re
import secrets
from threading import Thread


class ChromeBridgeError(RuntimeError):
    pass


class ChromePermissionBridge:
    def __init__(self, websocket_url):
        self._url = websocket_url
        self._token = secrets.token_hex(24)
        self._ready = Future()
        self._thread = None
        self._loop = None
        self._stop = None
        self._pending = {}
        self._forwarded = {}
        self._counter = 0
        self._browser = None
        self._page_sessions = {}
        self.address = None
        self.trace = None  # Optional protocol metadata for local adapter tests; never payloads.

    def _event(self, event, **fields):
        if self.trace is not None:
            self.trace({"event": event, **fields})

    def start(self):
        self._thread = Thread(target=self._run, daemon=True, name="sbi-chrome-connection")
        self._thread.start()
        try:
            self._ready.result(timeout=75)
        except Exception:
            self.close()
            raise ChromeBridgeError("Could not connect to permission-based Chrome; allow the connection in Chrome") from None
        return self

    def close(self):
        if self._loop is not None and self._stop is not None and not self._loop.is_closed():
            self._loop.call_soon_threadsafe(self._stop.set)
        if self._thread is not None:
            self._thread.join(timeout=5)

    def _run(self):
        try:
            asyncio.run(self._serve())
        except Exception:
            if not self._ready.done():
                self._ready.set_exception(ChromeBridgeError("Chrome connection failed"))

    def _next_id(self):
        self._counter += 1
        return self._counter

    async def _request(self, method, params=None):
        identifier = self._next_id()
        future = asyncio.get_running_loop().create_future()
        self._pending[identifier] = future
        try:
            await self._upstream.send(json.dumps({"id": identifier, "method": method, "params": params or {}}))
            reply = await asyncio.wait_for(future, timeout=60)
            if "error" in reply:
                raise ChromeBridgeError("Chrome rejected a discovery command")
            return reply.get("result", {})
        finally:
            self._pending.pop(identifier, None)

    async def _read_upstream(self):
        try:
            async for raw in self._upstream:
                message = json.loads(raw)
                self._event("upstream", method=message.get("method"), error=message.get("error", {}).get("code"))
                identifier = message.get("id")
                if identifier in self._pending:
                    future = self._pending[identifier]
                    if not future.done():
                        future.set_result(message)
                    continue
                route = self._forwarded.pop(identifier, None) if identifier is not None else None
                if identifier is not None and route is None:
                    continue
                if route is not None:
                    connection, original_id, direct_page = route
                    message["id"] = original_id
                    if direct_page:
                        message.pop("sessionId", None)
                else:
                    session = message.get("sessionId")
                    connection = self._page_sessions.get(session, self._browser)
                    if session in self._page_sessions:
                        message.pop("sessionId", None)
                if connection is not None:
                    try:
                        await connection.send(json.dumps(message))
                    except Exception:
                        pass  # One disconnected driver must not affect Chrome.
        finally:
            for future in self._pending.values():
                if not future.done():
                    future.set_exception(ChromeBridgeError("Chrome disconnected"))

    async def _serve(self):
        from websockets.asyncio.client import connect
        from websockets.asyncio.server import serve
        self._loop = asyncio.get_running_loop()
        self._stop = asyncio.Event()
        async with connect(self._url, proxy=None, open_timeout=60, max_size=32 * 1024 * 1024) as upstream:
            self._upstream = upstream
            reader = asyncio.create_task(self._read_upstream())
            try:
                self._version = await self._request("Browser.getVersion")
                webkit = re.search(r"AppleWebKit/([0-9.]+)", self._version["userAgent"])
                if webkit is None:
                    raise ChromeBridgeError("Chrome did not report its native WebKit version")
                self._webkit = webkit[1] + " (" + self._version["revision"] + ")"
                async with serve(self._handle, "127.0.0.1", 0, process_request=self._discovery,
                                 max_size=32 * 1024 * 1024, ping_interval=None) as server:
                    # ChromeDriver doesn't answer WebSocket ping frames while idle.
                    # The server's default 20s ping + 20s timeout invalidates the
                    # WebDriver session during Gmail/phone/AI waits. Match Chrome's
                    # CDP server by leaving driver-side keepalive disabled.
                    self.address = "127.0.0.1:" + str(server.sockets[0].getsockname()[1])
                    self._ready.set_result(True)
                    await self._stop.wait()
            finally:
                reader.cancel()
                await asyncio.gather(reader, return_exceptions=True)

    def _websocket_url(self, suffix):
        return "ws://" + self.address + "/" + self._token + "/devtools/" + suffix

    async def _discovery(self, connection, request):
        from websockets.datastructures import Headers
        from websockets.http11 import Response
        def response(code, phrase, data):
            body = json.dumps(data).encode()
            return Response(code, phrase, Headers({"Content-Type": "application/json", "Content-Length": str(len(body)),
                                                   "Cache-Control": "no-store"}), body)
        self._event("discovery", kind="websocket" if "/devtools/" in request.path else request.path,
                    origin_present=request.headers.get("Origin") is not None,
                    host_match=request.headers.get("Host") == self.address,
                    session_path=request.path.startswith("/" + self._token + "/devtools/"))
        port = self.address.rsplit(":", 1)[1]
        local_hosts = {self.address, "localhost:" + port, "localhost", "127.0.0.1"}
        if request.headers.get("Host") not in local_hosts or request.headers.get("Origin") is not None:
            return response(403, "Forbidden", {"error": "Local driver connections only"})
        if request.path.startswith("/" + self._token + "/devtools/"):
            return None
        if request.path == "/json/version":
            v = self._version
            return response(200, "OK", {"Browser": v["product"], "Protocol-Version": v["protocolVersion"],
                "User-Agent": v["userAgent"], "V8-Version": v["jsVersion"], "WebKit-Version": self._webkit,
                "webSocketDebuggerUrl": self._websocket_url("browser")})
        if request.path in ("/json", "/json/list"):
            targets = await self._request("Target.getTargets")
            return response(200, "OK", [{"id": t["targetId"], "type": t["type"], "title": t.get("title", ""),
                "url": t.get("url", ""), "webSocketDebuggerUrl": self._websocket_url("page/" + t["targetId"])}
                for t in targets.get("targetInfos", []) if t["type"] in ("page", "webview")])
        return response(404, "Not Found", {"error": "Unknown discovery endpoint"})

    async def _handle(self, connection):
        from websockets.exceptions import ConnectionClosed
        path = connection.request.path
        session = None
        if path == "/" + self._token + "/devtools/browser":
            self._event("connection", kind="browser", duplicate=self._browser is not None)
            if self._browser is not None:
                await connection.close(code=1008, reason="Driver already connected")
                return
            self._browser = connection
        elif path.startswith("/" + self._token + "/devtools/page/"):
            self._event("connection", kind="page")
            target = path.rsplit("/", 1)[1]
            targets = await self._request("Target.getTargets")
            if target not in {t["targetId"] for t in targets.get("targetInfos", [])}:
                await connection.close(code=1008, reason="Unknown target")
                return
            session = (await self._request("Target.attachToTarget", {"targetId": target, "flatten": True}))["sessionId"]
            self._page_sessions[session] = connection
        else:
            await connection.close(code=1008, reason="Unknown connection")
            return
        try:
            async for raw in connection:
                message = json.loads(raw)
                self._event("downstream", method=message.get("method"), direct_page=session is not None)
                identifier = message.get("id")
                if message.get("method") == "Browser.close":
                    await connection.send(json.dumps({"id": identifier, "error": {"code": -32000,
                        "message": "Closing the borrowed browser is disabled"}}))
                    continue
                if identifier is not None:
                    upstream_id = self._next_id()
                    self._forwarded[upstream_id] = (connection, identifier, session is not None)
                    message["id"] = upstream_id
                if session is not None:
                    message["sessionId"] = session
                await self._upstream.send(json.dumps(message))
        except ConnectionClosed:
            pass
        finally:
            if self._browser is connection:
                self._browser = None
            if session is not None:
                self._page_sessions.pop(session, None)
                try:
                    await self._request("Target.detachFromTarget", {"sessionId": session})
                except Exception:
                    pass
            for key in [key for key, route in self._forwarded.items() if route[0] is connection]:
                self._forwarded.pop(key, None)
