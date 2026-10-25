"""SBI preparation-only check, or user-started order/inquiry/cancel cycle with --execute."""

import argparse
import json
import logging
import math
import os
from pathlib import Path
import time
from datetime import datetime
import traceback
from contextlib import contextmanager
from queue import Empty, Queue
from threading import Thread

from dotenv import load_dotenv


ROOT = Path(__file__).resolve().parents[1]


class CycleStopped(RuntimeError):
    pass


class RunDiagnostics:
    """Record control flow and error locations, never values or raw exception messages."""

    def __init__(self, directory):
        directory = Path(directory)
        directory.mkdir(parents=True, exist_ok=True)
        stamp = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
        self.path = directory / f"sbi_order_cycle_{stamp}_{os.getpid()}.jsonl"
        self.path.touch(exist_ok=False)
        self.stage = "startup"
        self.started = time.monotonic()
        self.ai_requests = {}
        self.write_failed = False

    def write(self, event, **fields):
        record = {"time": datetime.now().astimezone().isoformat(timespec="milliseconds"),
                  "elapsed_seconds": round(time.monotonic() - self.started, 3),
                  "event": event, "stage": self.stage, **fields}
        try:
            with self.path.open("a", encoding="utf-8") as stream:
                stream.write(json.dumps(record, ensure_ascii=False) + "\n")
        except OSError:
            if not self.write_failed:
                print("Diagnostic log write failed; order actions will not be replayed.", flush=True)
            self.write_failed = True

    def set_stage(self, stage):
        self.stage = stage
        self.write("stage")

    def error(self, error, event="error"):
        message = str(error).lower()  # Inspect locally; never serialize it.
        reasons = {
            "credit_balance_exhausted": "credit_balance_exhausted",
            "insufficient_quota": "insufficient_quota",
            "complete login/authentication": "authentication_incomplete",
            "missing or cannot be recovered": "target_missing_or_ambiguous",
            "ambiguous": "ambiguous_target",
            "submission result is not verified": "submission_outcome_unknown",
            "already failed on this layout": "recovery_already_failed",
            "ai selenium locator recovery failed": "ai_locator_request_failed",
            "read recovery failed validation": "read_recovery_validation_failed",
            "recovered target is missing": "recovered_target_unavailable",
            "did not become interactable": "target_not_interactable",
        }
        reason = next((value for key, value in reasons.items() if key in message), "unclassified")
        phone_state = getattr(error, "phone_authentication_state", None)
        if phone_state in ("required", "timeout", "failed"):
            reason = "phone_authentication_" + phone_state
        if getattr(error, "order_not_submitted", False) is True:
            reason = "order_preparation_failed"
        code = getattr(error, "code", None)
        if code in ("credit_balance_exhausted", "insufficient_quota"):
            reason = code
        frames = [{"file": Path(frame.filename).name, "line": frame.lineno, "function": frame.name}
                  for frame in traceback.extract_tb(error.__traceback__)]
        causes = []
        seen = {id(error)}
        current = error.__cause__ or error.__context__
        while current is not None and id(current) not in seen and len(causes) < 5:
            seen.add(id(current))
            causes.append(type(current).__name__)
            current = current.__cause__ or current.__context__
        self.write(event, error_type=type(error).__name__, cause_types=causes, reason=reason, frames=frames)

    def observe_ai(self, recovery, channel):
        if recovery is None:
            return
        original = recovery.proposer
        def propose(*args, **kwargs):
            self.ai_requests[channel] = self.ai_requests.get(channel, 0) + 1
            self.write("ai_request", channel=channel)
            try:
                result = original(*args, **kwargs)
            except Exception as error:
                self.error(error, "ai_request_failed")
                raise
            self.write("ai_response", channel=channel)
            return result
        recovery.proposer = propose


def matching_orders(orders, symbol):
    return [order for order in orders if str(order["symbol_index"]) == symbol]


def wait_for_phone_call(timeout, *, driver=None, input_state=None):
    print("電話による追加認証が必要です。ブラウザーに表示された認証用番号へ、SBI登録済みの電話から発信してください。", flush=True)
    print(f"スクリプトの待機上限は残り約{max(0, int(timeout))}秒です。SBI画面に表示された有効期限内に操作してください。", flush=True)
    print("電話認証後、画面の「認証する」を手動で押すか、この端末でEnterを押してください。画面で完了すればEnterは不要です。注文は再送しません。", flush=True)
    started = time.monotonic()
    deadline = started + timeout
    input_state = {} if input_state is None else input_state
    answers = input_state.setdefault("answers", Queue())
    def read_answer():
        try:
            answer = input()
        except (EOFError, OSError):
            answer = None
        answers.put((time.monotonic(), answer))
    reader = input_state.get("reader")
    if reader is None or not reader.is_alive():
        reader = input_state["reader"] = Thread(target=read_answer, daemon=True)
        reader.start()
    while time.monotonic() < deadline:
        if driver is not None:
            from finance_client.sbi.sbi_util.sbi_util.phone_auth import phone_authentication_state
            state = phone_authentication_state(driver)
            if not state["required"]:
                return True
            if state["expired"]:
                return False
        try:
            answered_at, answer = answers.get(timeout=min(0.5, max(0, deadline - time.monotonic())))
            if answered_at < started:
                continue  # Do not consume input left over from a previous challenge.
            if answer == "":
                return True
            if answer is None and driver is None:
                return False
            # A noninteractive terminal must not prevent manual browser completion.
        except Empty:
            pass
    return False


@contextmanager
def exercise_preparation_recovery(stock, diagnostic, enabled):
    """Hide legacy lookup results, without modifying the broker page or clicking submit."""
    if not enabled:
        yield
        return
    from selenium.webdriver.common.by import By
    raw = stock.driver.raw_driver
    original = raw.find_elements
    targets = {(By.NAME, "input_quantity"): "quantity", (By.NAME, "input_price"): "price",
               (By.ID, "pwd3"): "trade_password", (By.ID, "shouryaku"): "confirmation_skip",
               (By.ID, "botton2"): "submit_button"}
    seen = set()
    def find_elements(by=By.ID, value=None):
        name = targets.get((by, value))
        if name is not None:
            if name not in seen:
                diagnostic.write("forced_locator_failure", target=name)
                try:
                    nodes = stock.driver.recovery.snapshot(raw, None)
                    # Record only the sanitized neighborhood of the original target.
                    elements = original(by, value)
                    if len(elements) == 1:
                        path = raw.execute_script("""
                            const parts=[]; for(let e=arguments[0];e && e.nodeType===1;e=e.parentElement) {
                              let n=1; for(let s=e.previousElementSibling;s;s=s.previousElementSibling) if(s.tagName===e.tagName)n++;
                              parts.push(e.tagName.toLowerCase()+'['+n+']');
                            } return '/'+parts.reverse().join('/');
                        """, elements[0])
                        parent = path.rsplit("/", 2)[0] + "/"
                        diagnostic.write("forced_locator_context", target=name,
                                         nodes=[n for n in nodes if n["path"].startswith(parent)])
                except Exception as error:
                    diagnostic.write("forced_locator_context_unavailable", error_type=type(error).__name__)
                seen.add(name)
            return []
        return original(by, value)
    raw.find_elements = find_elements
    try:
        yield
    finally:
        raw.find_elements = original


def run_preparation(stock, symbol, units, price, *, report=None):
    report = report or (lambda stage: None)
    report("check_existing_orders")
    stock.get_orders()
    report("prepare_order")
    if not stock.buy_order(symbol, units, order_price=price, submit=False):
        raise CycleStopped("Order form preparation did not succeed. No final submission was requested.")
    report("order_ready_without_submission")
    return {"order_form_verified": True, "submission_button_ready": True, "order_submitted": False}


def run_cycle(stock, symbol, units, price, *, pause=time.sleep, report=None):
    report = report or (lambda stage: None)
    report("check_existing_orders")
    if matching_orders(stock.get_orders(), symbol):
        raise CycleStopped("Existing orders for this security: no new order was sent.")
    print("Submitting one order...", flush=True)
    report("submit_order")
    if not stock.buy_order(symbol, units, order_price=price):
        raise CycleStopped("Order submission did not succeed. Check SBI before any retry.")
    found = False
    report("verify_open_order")
    for attempt in range(6):
        orders = matching_orders(stock.get_orders(), symbol)
        if len(orders) > 1:
            raise CycleStopped("Multiple matching orders: cancellation was not attempted.")
        if orders:
            found = True
            break
        if attempt < 5:
            pause(2)
    if not found:
        raise CycleStopped("Submitted order was not verified in the open-order list. Check SBI; do not resubmit.")
    print("Open order verified. Sending cancellation once...", flush=True)
    report("cancel_order")
    if not stock.cancel_order(symbol):
        raise CycleStopped("Cancellation did not succeed. Check SBI; do not repeat the cycle.")
    report("verify_cancellation")
    for attempt in range(6):
        if not matching_orders(stock.get_orders(), symbol):
            report("completed")
            return {"order_accepted": True, "open_order_verified": True,
                    "cancellation_confirmed": True, "remaining_matching_orders": 0}
        if attempt < 5:
            pause(2)
    raise CycleStopped("Matching orders remain after cancellation. Check their status in SBI.")


def main():
    parser = argparse.ArgumentParser(description="SBIの実口座で指値発注→未約定照会→取消を1回実行します")
    parser.add_argument("--symbol", required=True, help="ご自身で選んだ4桁の数字の銘柄コード")
    parser.add_argument("--units", required=True, type=int, help="売買単位数。株数ではありません")
    parser.add_argument("--price", required=True, type=float, help="ご自身で選んだ指値（円）")
    parser.add_argument("--env-file", type=Path, default=ROOT / ".env", help="設定ファイル。既定はfinance_client直下の.env")
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--execute", action="store_true", help="実口座への発注・取消を実行。省略時は設定確認のみ")
    mode.add_argument("--prepare-only", action="store_true", help="実画面の注文入力と発注ボタンの有効状態を確認し、最終ボタンは押さない")
    parser.add_argument("--exercise-recovery", action="store_true", help="prepare-only時に既存検索を失敗させ、復旧経路も確認する")
    args = parser.parse_args()
    if args.exercise_recovery and not args.prepare_only:
        parser.error("--exercise-recovery requires --prepare-only")
    if not (args.symbol.isascii() and args.symbol.isdigit() and len(args.symbol) == 4):
        parser.error("--symbol must be a four-digit numeric code")
    if args.units <= 0 or not math.isfinite(args.price) or args.price <= 0:
        parser.error("--units and --price must be positive finite values")
    load_dotenv(args.env_file, override=False, encoding="utf-8-sig", interpolate=False)
    required = ("sbi_id", "sbi_password", "sbi_trade_password", "SBI_AI_MODEL", "OPENAI_API_KEY")
    missing = [name for name in required if not os.getenv(name)]
    if os.getenv("SBI_AI_HEALING") != "1":
        missing.append("SBI_AI_HEALING=1")
    if os.getenv("SBI_AI_OPERATION_HEALING", "1") != "1":
        missing.append("SBI_AI_OPERATION_HEALING=1")
    if missing:
        parser.error("Missing .env settings: " + ", ".join(missing))
    print(json.dumps({"symbol": args.symbol, "units": args.units, "limit_price": args.price,
                      "execute": args.execute, "prepare_only": args.prepare_only,
                      "exercise_recovery": args.exercise_recovery}, ensure_ascii=False), flush=True)
    if not args.execute and not args.prepare_only:
        print("Configuration check only. No browser, order or AI request was started.", flush=True)
        return 0

    return execute_cycle(args)


def execute_cycle(args):
    try:
        diagnostic = RunDiagnostics(ROOT / "logs")
    except OSError:
        print("Cannot create diagnostic log. No broker connection was started.", flush=True)
        return 1
    print("Diagnostic log: " + str(diagnostic.path), flush=True)
    previous_disable = logging.root.manager.disable
    # Legacy logging can contain account values. Use the separate structured log instead.
    logging.disable(logging.CRITICAL)
    stock = None
    try:
        diagnostic.write("mode", mode="prepare_only" if getattr(args, "prepare_only", False) else "execute")
        diagnostic.set_stage("load_client")
        from finance_client.sbi.sbi_util.sbi_util.rpa import STOCK
        from finance_client.sbi.sbi_util.sbi_util.read_recovery import ReadRecovery
        from finance_client.sbi.sbi_util.sbi_util.selenium_recovery import SeleniumRecovery
        read_recovery = ReadRecovery.from_env()
        operation_recovery = SeleniumRecovery.from_env()
        diagnostic.observe_ai(read_recovery, "read")
        diagnostic.observe_ai(operation_recovery, "selenium")
        if operation_recovery is not None:
            operation_recovery.on_auth_error = diagnostic.error
            operation_recovery.on_preparation_error = diagnostic.error
            operation_recovery.on_submission_result = lambda **fields: diagnostic.write("submission_result", **fields)
        diagnostic.set_stage("login")
        stock = STOCK(read_only=False, read_recovery=read_recovery, operation_recovery=operation_recovery,
                      phone_auth_handler=None)
        if not getattr(args, "prepare_only", False):
            phone_input = {}
            stock.phone_auth_handler = lambda timeout: wait_for_phone_call(
                timeout, driver=stock.driver, input_state=phone_input)
        ReadRecovery._host(stock.driver)
        runner = run_preparation if getattr(args, "prepare_only", False) else run_cycle
        with exercise_preparation_recovery(stock, diagnostic, getattr(args, "exercise_recovery", False)):
            result = runner(stock, args.symbol, args.units, args.price, report=diagnostic.set_stage)
        print(json.dumps(result, ensure_ascii=False), flush=True)
        return 0
    except Exception as error:
        diagnostic.error(error)
        if getattr(args, "prepare_only", False) and stock is not None and hasattr(stock.driver, "recovery"):
            stock._record_submission_result("order", "not_submitted", error)
        if getattr(error, "order_not_submitted", False) is True:
            print("Stopped: " + type(error).__name__ + ". No order or cancellation was submitted.", flush=True)
        elif isinstance(error, CycleStopped):
            print(str(error), flush=True)
        elif type(error).__name__ == "ChromeConfigurationError":
            print(str(error), flush=True)
        elif getattr(error, "phone_authentication_state", None) in ("required", "timeout", "failed"):
            print("電話認証が未完了のため停止しました。注文・取消は再送していません。SBIで注文状況を確認してください。", flush=True)
        elif getattr(args, "prepare_only", False):
            print("Preparation stopped: " + type(error).__name__ + ". No final submission was requested.", flush=True)
        elif diagnostic.stage in ("startup", "load_client", "login", "check_existing_orders"):
            print("Stopped: " + type(error).__name__ + ". No order or cancellation was submitted.", flush=True)
        else:
            print("Stopped: " + type(error).__name__ + ". Check the order status in SBI before retrying.", flush=True)
        return 1
    finally:
        if stock is not None and hasattr(stock, "driver"):
            try:
                stock.driver.quit()
            except Exception as error:
                diagnostic.error(error, "browser_close_failed")
            finally:
                del stock.driver
        diagnostic.write("finished", ai_requests=diagnostic.ai_requests)
        logging.disable(previous_disable)


if __name__ == "__main__":
    raise SystemExit(main())
