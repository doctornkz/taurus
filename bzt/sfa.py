"""
Based on bza.py (Blazemeter API client) module, and adopted as-is for SignalFX API
"""
import base64
import json
import logging
from collections import OrderedDict

import requests

from bzt import TaurusNetworkError, ManualShutdown, VERSION, TaurusException
from bzt.six import string_types
from bzt.six import text_type
from bzt.six import urlencode
from bzt.utils import to_json, MultiPartForm

SFA_TEST_DATA_RECEIVED = 100

class SFAObject(dict):
    def __init__(self, proto=None, data=None):
        """
        :type proto: SFAObject
        :type data: dict
        """
        super(SFAObject, self).__init__()
        self.update(data if data is not None else {})
        # FIXME: Hardcode?
        self.address = "https://myrealm.signalfx.com"
        self.data_address = "https://ingest.eu0.signalfx.com"
        self.dashboard_url = 'https://<REALM>.signalfx.com/#/dashboard/<ID>'
        self.timeout = 30
        self.logger_limit = 256
        self.token = None
        self.log = logging.getLogger(self.__class__.__name__)
        self.http_session = requests.Session()
        self.http_request = self.http_session.request
        self._retry_limit = 5
        self.uuid = None

        # copy infrastructure from prototype
        if isinstance(proto, SFAObject):
            attrs_own = set(dir(SFAObject()))
            attrs_parent = set(dir(SFAObject.__bases__[0]()))
            attrs_diff = attrs_own - attrs_parent  # get only SFAObject attrs
            for attr in attrs_diff:
                if attr.startswith('__') or attr in (self._request.__name__,):
                    continue
                self.__setattr__(attr, proto.__getattribute__(attr))

    def _request(self, url, data=None, headers=None, method=None, raw_result=False, retry=True):
        """
        :param url: str
        :type data: Union[dict,str]
        :param headers: dict
        :param method: str
        :return: dict
        """
        if not headers:
            headers = {}

        has_auth = headers and "X-SF-TOKEN" in headers
        if has_auth:
            pass  # all is good, we have auth provided
        elif isinstance(self.token, string_types):
            token = self.token
            headers["X-SF-TOKEN"] = self.token

        if method:
            log_method = method
        else:
            log_method = 'GET' if data is None else 'POST'

        url = str(url)

        if isinstance(data, text_type):
            data = data.encode("utf-8")

        if isinstance(data, (dict, list)):
            data = to_json(data)
            headers["Content-Type"] = "application/json"

        self.log.debug("Request: %s %s %s", log_method, url, data[:self.logger_limit] if data else None)

        retry_limit = self._retry_limit

        while True:
            try:
                response = self.http_request(
                    method=log_method, url=url, data=data, headers=headers, timeout=self.timeout)
            except requests.ReadTimeout:
                if retry and retry_limit:
                    retry_limit -= 1
                    self.log.warning("ReadTimeout: %s. Retry..." % url)
                    continue
                raise
            break

        resp = response.content
        if not isinstance(resp, str):
            resp = resp.decode()

        self.log.debug("Response [%s]: %s", response.status_code, resp[:self.logger_limit] if resp else None)
        if response.status_code >= 400:
            try:
                result = json.loads(resp) if len(resp) else {}
                if 'error' in result and result['error']:
                    raise TaurusNetworkError("API call error %s: %s" % (url, result['error']))
                else:
                    raise TaurusNetworkError("API call error %s on %s: %s" % (response.status_code, url, result))
            except ValueError:
                raise TaurusNetworkError("API call error %s: %s %s" % (url, response.status_code, response.reason))

        if raw_result:
            return resp

        try:
            result = json.loads(resp) if len(resp) else {}
        except ValueError as exc:
            self.log.debug('Response: %s', resp)
            raise TaurusNetworkError("Non-JSON response from API: %s" % exc)

        if 'error' in result and result['error']:
            raise TaurusNetworkError("API call error %s: %s" % (url, result['error']))

        return result

class Session(SFAObject):
    def __init__(self, proto=None, data=None):
        super(Session, self).__init__(proto, data)
        self.data_signature = None
        self.kpi_target = 'labels_bulk'
        #self.monitoring_upload_notified = False
        

    def set(self, data):
        url = self.address + "/api/v4/sessions/%s" % self['id']
        res = self._request(url, data, method='POST')
        self.log.info("UUID: %s", self['id'])
        self.log.info("URL: %s", url)
        self.log.info("RESP: %s", url)
        self.update(res['result'])

    def ping(self, token):
        """ Quick check if we can access the service """
        hdr = {"Content-Type": "application/json", "X-SF-TOKEN": token}
        url = self.address + '/v2/organization'
        self._request(url, headers=hdr)

    def send_kpi_data(self, data, is_check_response=True, submit_target=None):
        """
        Sends online data

        :type submit_target: str
        :param is_check_response:
        :type data: str
        """
        #submit_target = self.kpi_target if submit_target is None else submit_target
        #self.log.info("Payload: %s", data)
        
        url = self.data_address
        hdr = {"Content-Type": "application/json"}
        response = self._request(url, data, headers=hdr)


        if response and 'response_code' in response and response['response_code'] != 200:
            raise TaurusNetworkError("Failed to feed data to %s, response code %s" %
                                     (submit_target, response['response_code']))

        if response and 'result' in response and is_check_response:
            result = response['result']['session']
            self.log.debug("Result: %s", result)
            if 'statusCode' in result and result['statusCode'] > 100:
                self.log.info("Test was stopped through Web UI: %s", result['status'])
                raise ManualShutdown("The test was interrupted through Web UI")

