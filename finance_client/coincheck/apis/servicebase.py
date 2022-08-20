import http.client
import logging
import urllib
import time
import hmac
import hashlib
from dotenv import load_dotenv
import os

class ServiceBase:
    METHOD_GET = 'GET'
    METHOD_POST = 'POST'
    METHOD_DELETE = 'DELETE'
    
    __singleton = None
    ACCESS_ID_KEY = 'ACCESS_ID'
    ACCESS_SECRET_KEY = 'ACCESS_SECRET'

    def __new__(cls, ACCESS_ID=None, ACCESS_SECRET=None):
        if cls.__singleton is None:
            #print("singleton")
            cls.__singleton = super(ServiceBase, cls).__new__(cls)
            cls.DEBUG = False
            cls.apiBase = 'coincheck.jp'
            if ACCESS_ID is None and ACCESS_SECRET is None:
                load_dotenv()
                if  cls.ACCESS_ID_KEY not in os.environ or cls.ACCESS_SECRET_KEY not in os.environ:
                    raise Exception("Coin Check Credentials is required.")
            else:
                os.environ[cls.ACCESS_ID_KEY] = ACCESS_ID
                os.environ[cls.ACCESS_SECRET_KEY] = ACCESS_SECRET
        return cls.__singleton

    def __setSignature__(self, request_headers, path):
        url = 'https://' + self.apiBase + path
        creds_header = self.create_credential_header(url=url)
        request_headers.update(creds_header)

        if (self.DEBUG):
            self.logger.info(f'Set signature: {creds_header}')
    
    def create_credential_header(self, url):
        nonce = str(round(time.time() * 1000000))
        message = nonce + url
        signature = hmac.new(os.environ[self.ACCESS_SECRET_KEY].encode('utf-8'), message.encode('utf-8'), hashlib.sha256).hexdigest()
        header = {
            'ACCESS-NONCE': nonce,
            'ACCESS-KEY': os.environ[self.ACCESS_ID_KEY],
            'ACCESS-SIGNATURE': signature
        }
        return header

    def request(self, method, path, params = {}):
        if (method == 'GET' and len(params) > 0):
            path = path + '?' + urllib.parse.urlencode(params)
        data = ''
        request_headers = {}
        if (method == 'POST' or method == 'DELETE'):
            request_headers = {
                'content-type': "application/json"
            }
        self.__setSignature__(request_headers, path)

        client = http.client.HTTPSConnection(self.apiBase)
        if (self.DEBUG):
            self.logger.info('Process request...')
        client.request(method, path, data, request_headers)
        res = client.getresponse()
        data = res.read()
        return data.decode("utf-8")