import ast
import hashlib
import hmac
import http.client
import json
import logging
import os
import time
import urllib

logger = logging.getLogger(__name__)


class ServiceBase:
    METHOD_GET = "GET"
    METHOD_POST = "POST"
    METHOD_DELETE = "DELETE"

    __singleton = None
    ACCESS_ID_KEY = "ACCESS_ID"
    ACCESS_SECRET_KEY = "ACCESS_SECRET"

    def __new__(cls, ACCESS_ID=None, ACCESS_SECRET=None):
        if cls.__singleton is None:
            cls.__singleton = super(ServiceBase, cls).__new__(cls)
            cls.DEBUG = False
            cls.apiBase = "coincheck.com"
            # print("singleton")
            if ACCESS_ID is None and ACCESS_SECRET is None:
                pass
                # load_dotenv()
                # if  cls.ACCESS_ID_KEY not in os.environ or cls.ACCESS_SECRET_KEY not in os.environ:
                #     raise Exception("Coin Check Credentials is required.")
            else:
                os.environ[cls.ACCESS_ID_KEY] = ACCESS_ID
                os.environ[cls.ACCESS_SECRET_KEY] = ACCESS_SECRET
                print("credentials are stored.")
        elif cls.ACCESS_ID_KEY not in os.environ and ACCESS_SECRET is not None:
            os.environ[cls.ACCESS_ID_KEY] = ACCESS_ID
            os.environ[cls.ACCESS_SECRET_KEY] = ACCESS_SECRET
            print("credentials are stored.")
        return cls.__singleton

    def __setSignature(self, request_headers, path, body=None):
        url = "https://" + self.apiBase + path
        creds_header = self.create_credential_header(url=url, body=body)
        request_headers.update(creds_header)

        logger.debug(f"Set signature: {creds_header}")

    def create_credential_header(self, url, body=None):
        nonce = str(round(time.time() * 1000000))
        message = nonce + url
        if body is not None:
            message += body
        signature = hmac.new(os.environ[self.ACCESS_SECRET_KEY].encode("utf-8"), message.encode("utf-8"), hashlib.sha256).hexdigest()
        header = {"ACCESS-NONCE": nonce, "ACCESS-KEY": os.environ[self.ACCESS_ID_KEY], "ACCESS-SIGNATURE": signature}
        return header

    def request(self, method, path, query_params={}, _body: dict = None):
        if len(query_params) > 0:
            path = path + "?" + urllib.parse.urlencode(query_params)
        body = ""
        if _body is not None:
            body = json.dumps(_body)
        request_headers = {"content-type": "application/json"}
        self.__setSignature(request_headers, path, body=body)

        client = http.client.HTTPSConnection(self.apiBase)
        logger.debug("Process request...")
        client.request(method, path, body, request_headers)
        res = client.getresponse()
        data = res.read()
        client.close()
        return data.decode("utf-8")

    def parse_str_to_dict(self, _str):
        try:
            return ast.literal_eval(_str)
        except Exception:
            pass

        try:
            return json.loads(_str)
        except Exception as e:
            print(f"couldn't parse: {e}")
        return _str

    def check_response(self, response_dict):
        # TODO: define common error
        if "success" in response_dict:
            result = response_dict["success"]
            if result is False:
                err_txt = response_dict["error"]
                print(f"error on CoinCheck API: {result}")
                if err_txt == "invalid authentication":
                    raise Exception(err_txt)
                elif "所持金額が足りません" in err_txt:
                    raise ValueError("budget is insufficient for the volume")
