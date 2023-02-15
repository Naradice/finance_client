import json
import threading
import time
from datetime import datetime

import websocket


class CoinCheckWebSocket:
    URL = "wss://ws-api.coincheck.com/"
    MAX_CANDLE_LEN = 24 * 60

    def __init__(self):
        self.__candle = []
        self.__init_flg = False

        self.__f = open("coin1m.log", "w")

        self.__lock = threading.Lock()

        self.__connect()

    def __connect(self):
        self.__connected = False
        self.__opened = False
        self.ws = websocket.WebSocketApp(
            CoinCheckWebSocket.URL, on_message=self.__on_message, on_close=self.__on_close, on_open=self.__on_open, on_error=self.__on_error
        )

        self.wst = threading.Thread(target=lambda: self.ws.run_forever())
        self.wst.daemon = True
        self.wst.start()

        # Candle Thread
        self._check_candle_thread = threading.Thread(target=self.__check_candle, args=("check_candle",))
        self._check_candle_thread.daemon = True
        self._check_candle_thread.start()
        print("check candle thread start")

    def __on_open(self, ws):
        print("open")

        self.ws.send(json.dumps({"type": "subscribe", "channel": "btc_jpy-trades"}))

        self.__opened = True

    def __on_error(self, ws, error):
        print("error")
        print(error)
        self.__reconnect()

    def __on_close(self, ws):
        print("close, why close")

    def __on_message(self, ws, message):
        # print(message)
        if self.__connected is False:
            self.__connected = True

        trade = self.__make_trade(message)
        # print(trade)
        if self.__init_flg is False:
            self.__init_candle_data(trade)
            self.__init_flg = True
        else:
            self.__update_candle_data(trade)

    def __candle_thread_terminate(self):
        print("__candle_thread_terminate invoked")
        self.__connected = True  # waitで止まっていることもあるため
        time.sleep(1.5)

    def __exit(self):
        print("__exit invoked")
        self.ws.close()
        self.__candle_thread_terminate()

    def __reconnect(self):
        print("__reconnect invoked")
        self.__exit()
        time.sleep(2)
        self.__connect()

    def __make_trade(self, message):
        elements = message.split(",")
        trade = {
            "id": int(elements[0][1 : len(elements[0]) - 1]),
            "price": float(elements[2][1 : len(elements[2]) - 1]),
            "volume": float(elements[3][1 : len(elements[3]) - 1]),
            "type": elements[4][1 : len(elements[4]) - 2],
        }
        return trade

    def __thread_lock(self):
        _count = 0
        while self.__lock.acquire(blocking=True, timeout=1) is False:
            _count += 1
            if _count > 3:
                print("lock acquite timeout")
                return False
        return True

    def __thread_unlock(self):
        try:
            self.__lock.release()
        except Exception as e:
            print("lock release a {}".format(e))
            return False
        return True

    def __format_candle(self, candle):
        dt = datetime.fromtimestamp(candle["timestamp"])
        s_str = "{0:%Y-%m-%d %H:%M:%S}".format(dt)
        fmt_str = "%s  %.1f  %.1f  %.1f  %.1f   %.6f   %.6f   %.6f" % (
            s_str,
            candle["open"],
            candle["high"],
            candle["low"],
            candle["close"],
            candle["volume"],
            candle["buy"],
            candle["sell"],
        )
        return fmt_str

    def _write_log(self, candle):
        fmt_str = self.__format_candle(candle)
        fmt_str += "\r\n"
        self.__f.write(fmt_str)
        self.__f.flush()

    def _print_log(self, candle):
        fmt_str = self.__format_candle(candle)
        print(fmt_str)

    def __init_candle_data(self, trade):
        _dt = datetime.now().replace(second=0, microsecond=0)
        _stamp = _dt.timestamp()
        self.__candle.append(
            {
                "timestamp": _stamp,
                "open": trade["price"],
                "high": trade["price"],
                "low": trade["price"],
                "close": trade["price"],
                "volume": trade["volume"],
                "buy": trade["volume"] if trade["type"] == "buy" else 0,
                "sell": trade["volume"] if trade["type"] == "sell" else 0,
            }
        )

    def __update_candle_data(self, trade):
        last_candle = self.__candle[-1]
        _dt = datetime.now().replace(second=0, microsecond=0)
        mark_ts = _dt.timestamp()
        last_ts = last_candle["timestamp"]
        if last_ts == mark_ts:
            print("append")
            last_candle["high"] = max(last_candle["high"], trade["price"])
            last_candle["low"] = min(last_candle["low"], trade["price"])
            last_candle["close"] = trade["price"]
            last_candle["volume"] += trade["volume"]
            last_candle["buy"] += trade["volume"] if trade["type"] == "buy" else 0
            last_candle["sell"] += trade["volume"] if trade["type"] == "sell" else 0

            self._print_log(last_candle)
        else:
            print("add new")
            self._write_log(last_candle)
            self.__candle.append(
                {
                    "timestamp": mark_ts,
                    "open": trade["price"],
                    "high": trade["price"],
                    "low": trade["price"],
                    "close": trade["price"],
                    "volume": trade["volume"],
                    "buy": trade["volume"] if trade["type"] == "buy" else 0,
                    "sell": trade["volume"] if trade["type"] == "sell" else 0,
                }
            )

    def get_candle(self, type=0):
        self.__thread_lock()
        if type == 0:
            candle = self.__candle[:-1]
        else:
            candle = self.__candle[:]
        self.__thread_unlock()
        return candle

    def __check_candle(self, args):
        _error_count = 0
        while True:
            if not self.__connected:
                _error_count += 1
                print("wait 1 sec")
                time.sleep(1)

                if not self.__opened and _error_count > 3:
                    # print("nonono reconnect!!!")
                    self.ws.on_error = None  # 2回呼ばれることを回避
                    term_thread = threading.Thread(target=lambda: self.__reconnect())  # 別スレッドでやらないと，このスレッドを終了できない
                    term_thread.start()
                    break

            else:
                break

        _timer_count = 0
        while self.ws.sock and self.ws.sock.connected:
            time.sleep(1)
            if _timer_count < 30:
                print("wait until 30")
                _timer_count += 1
                continue

            print(">>>>>>check candle")

            self.__thread_lock()

            _dt = datetime.now().replace(second=0, microsecond=0)
            mark_ts = _dt.timestamp()
            last_candle = self.__candle[-1]
            last_ts = last_candle["timestamp"]
            # 現在時刻の1分範囲じゃない
            if last_ts != mark_ts:
                print("---->>>>>>>  new in check candle")
                self._write_log(last_candle)
                self.__candle.append(
                    {
                        "timestamp": mark_ts,
                        "open": last_candle["close"],
                        "high": last_candle["close"],
                        "low": last_candle["close"],
                        "close": last_candle["close"],
                        "volume": 0,
                        "buy": 0,
                        "sell": 0,
                    }
                )
            if len(self.__candle) > (CoinCheckWebSocket.MAX_CANDLE_LEN * 1.5):
                self.__candle = self.__candle[-CoinCheckWebSocket.MAX_CANDLE_LEN :]

            self.__thread_unlock()
            _timer_count = 0

        print("check candle end")


if __name__ == "__main__":
    chs = CoinCheckWebSocket()

    while True:
        time.sleep(60)
