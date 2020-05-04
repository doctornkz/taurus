"""
Module for reporting into http://www.blazemeter.com/ service

Copyright 2015 BlazeMeter Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
"""
import copy
import logging
import os
import platform
import re
import sys
import time
import traceback
import zipfile
import uuid
from abc import abstractmethod
from collections import defaultdict, OrderedDict, Counter, namedtuple
from functools import wraps
from ssl import SSLError

import requests
import yaml
from requests.exceptions import ReadTimeout
from terminaltables import SingleTable, AsciiTable
from urwid import Pile, Text

from bzt import AutomatedShutdown
from bzt import TaurusInternalException, TaurusConfigError, TaurusException, TaurusNetworkError, NormalShutdown
from bzt.sfa import Session, SFA_TEST_DATA_RECEIVED

from bzt.engine import Reporter, Provisioning, Configuration, Service
from bzt.engine import Singletone, SETTINGS, ScenarioExecutor, EXEC
from bzt.modules.aggregator import DataPoint, KPISet, ConsolidatingAggregator, ResultsProvider, AggregatorListener
from bzt.modules.console import WidgetProvider, PrioritizedWidget
from bzt.modules.functional import FunctionalResultsReader, FunctionalSample
from bzt.modules.services import Unpacker
from bzt.modules.selenium import SeleniumExecutor
from bzt.requests_model import has_variable_pattern
from bzt.six import BytesIO, iteritems, HTTPError, r_input, URLError, b, string_types, text_type
from bzt.utils import open_browser, BetterDict, ExceptionalDownloader, ProgressBarContext
from bzt.utils import to_json, dehumanize_time, get_full_path, get_files_recursive, replace_in_config, humanize_bytes


NETWORK_PROBLEMS = (IOError, URLError, SSLError, ReadTimeout, TaurusNetworkError)
NOTE_SIZE_LIMIT = 2048


def send_with_retry(method):
    @wraps(method)
    def _impl(self, *args, **kwargs):
        if not isinstance(self, SignalfxUploader):
            raise TaurusInternalException("send_with_retry should only be applied to SignalfxUploader methods")

        try:
            method(self, *args, **kwargs)
        except (IOError, TaurusNetworkError):
            self.log.debug("Error sending data: %s", traceback.format_exc())
            self.log.warning("Failed to send data, will retry in %s sec...", self._session.timeout)
            try:
                time.sleep(self._session.timeout)
                method(self, *args, **kwargs)
                self.log.info("Succeeded with retry")
            except NETWORK_PROBLEMS:
                self.log.error("Fatal error sending data: %s", traceback.format_exc())
                self.log.warning("Will skip failed data and continue running")

    return _impl



class SignalfxUploader(Reporter, AggregatorListener, Singletone):
    """
    Reporter class

    :type _session: bzt.sfa.Session
    """

    def __init__(self):
        super(SignalfxUploader, self).__init__()
        self.browser_open = 'start'
        self.project = 'myproject'
        self.custom_tags = {}
        self.additional_tags = {}
        self.kpi_buffer = []
        self.send_interval = 30
        self._last_status_check = time.time()
        self.send_data = True
        self.upload_artifacts = True
        self.send_monitoring = True
        self.monitoring_buffer = None
        self.public_report = False
        self.last_dispatch = 0
        self.results_url = None
        self._test = None
        self._master = None
        self._session = None
        self.first_ts = sys.maxsize
        self.last_ts = 0
        self.report_name = None
        self._dpoint_serializer = DatapointSerializer(self)

    def prepare(self):
        """
        Read options for uploading, check that they're sane
        """
        super(SignalfxUploader, self).prepare()
        self.send_interval = dehumanize_time(self.settings.get("send-interval", self.send_interval))
        self.browser_open = self.settings.get("browser-open", self.browser_open)
        self.project = self.settings.get("project", self.project)
        self.custom_tags = self.settings.get("custom_tags", self.custom_tags)
        self.public_report = self.settings.get("public-report", self.public_report)
        self.upload_artifacts = self.parameters.get("upload-artifacts", self.upload_artifacts)
        self._dpoint_serializer.multi = self.settings.get("report-times-multiplier", self._dpoint_serializer.multi)
        token = self.settings.get("token", "")
        if not token:
            self.log.warning("No SignalFX API key provided, only local results are available")
            exit(1)
        

        # direct data feeding case
        self.sess_id = str(uuid.uuid4())
        self.additional_tags.update({'project': self.project, 'uuid': self.sess_id})
        self.additional_tags.update(self.custom_tags)

        if not self._session:
            self._session = Session()
            self._session['id'] = self.sess_id
            self._session.token = token
            self._session.address = self.settings.get("address", self._session.address).rstrip("/")
            self._session.timeout = dehumanize_time(self.settings.get("timeout", self._session.timeout))
            try:
                self._session.ping(self._session.token)  # to check connectivity and auth
            except Exception:
                self.log.error("Cannot reach SignalFX API, maybe the address/token is wrong")
                raise

        self._session.dashboard_url = self.settings.get("dashboard_url", self._session.dashboard_url).rstrip("/")
        self._session.data_address = self.settings.get("data-address", self._session.data_address).rstrip("/")

        if isinstance(self.engine.aggregator, ResultsProvider):
            self.engine.aggregator.add_listener(self)

    def startup(self):
        """
        Initiate online test
        """
        super(SignalfxUploader, self).startup()
        self._session.log = self.log.getChild(self.__class__.__name__)

        if not self._session:
            url = self._start_online()
            self.log.info("Started data feeding: %s", url)
            if self.browser_open in ('start', 'both'):
                open_browser(url)

            if self._session.token and self.public_report:
                report_link = self._master.make_report_public()
                self.log.info("Public report link: %s", report_link)

    def _start_online(self):
        """
        Start online test

        """
        self.log.info("Initiating data feeding...")

        self.results_url = self.dashboard_url + \
            '?startTime=-15m&endTime=Now' + \
            '&sources%5B%5D=' + \
            'project:' + \
            self.project + \
            '&sources%5B%5D=uuid:' + \
            self.sess_id + \
            '&density=4'
        return self.results_url


    def post_process(self):
        """
        Upload results if possible
        """
        if not self._session:
            self.log.debug("No feeding session obtained, nothing to finalize")
            return

        self.log.debug("KPI bulk buffer len in post-proc: %s", len(self.kpi_buffer))
        self.startup()
        try:
            self.log.info("Sending remaining KPI data to server...")
            if self.send_data:
                self.__send_data(self.kpi_buffer, False, True)
                self.kpi_buffer = []

        finally:
            self._postproc_phase2()

        if self.results_url:
            if self.browser_open in ('end', 'both'):
                open_browser(self.results_url)
            self.log.info("Online report link: %s", self.results_url)

    def _postproc_phase2(self):
        try:
            if self.send_data:
                self.end_online()

            if self._session.token and self.engine.stopping_reason:
                exc_class = self.engine.stopping_reason.__class__.__name__
                note = "%s: %s" % (exc_class, str(self.engine.stopping_reason))
                self.append_note_to_session(note)
                if self._master:
                    self.append_note_to_master(note)

        except KeyboardInterrupt:
            raise
        except BaseException as exc:
            self.log.debug("Failed to finish online: %s", traceback.format_exc())
            self.log.warning("Failed to finish online: %s", exc)

    def end_online(self):
        """
        Finish online test
        """
        if not self._session:
            self.log.debug("Feeding not started, so not stopping")
        else:
            self.log.info("Ending data feeding...")

    def append_note_to_session(self, note):
        self._session.fetch()
        if 'note' in self._session:
            note = self._session['note'] + '\n' + note
        note = note.strip()
        if note:
            self._session.set({'note': note[:NOTE_SIZE_LIMIT]})

    def append_note_to_master(self, note):
        self._master.fetch()
        if 'note' in self._master:
            note = self._master['note'] + '\n' + note
        note = note.strip()
        if note:
            self._master.set({'note': note[:NOTE_SIZE_LIMIT]})

    def check(self):
        """
        Send data if any in buffer
        """
        self.log.debug("KPI bulk buffer len: %s", len(self.kpi_buffer))
        if self.last_dispatch < (time.time() - self.send_interval):
            self.last_dispatch = time.time()
            if self.send_data and len(self.kpi_buffer):
                self.__send_data(self.kpi_buffer)
                self.kpi_buffer = []
        return super(SignalfxUploader, self).check()

    @send_with_retry
    def __send_data(self, data, do_check=True, is_final=False):
        """
        :type data: list[bzt.modules.aggregator.DataPoint]
        """

        serialized = self._dpoint_serializer.get_kpi_body(data, self.additional_tags, is_final)
        self._session.send_kpi_data(serialized, do_check)

    def aggregated_second(self, data):
        """
        Send online data
        :param data: DataPoint
        """
        if self.send_data:
            self.kpi_buffer.append(data)
            
    def __format_listing(self, zip_listing):
        lines = []
        for fname in sorted(zip_listing.keys()):
            bytestr = humanize_bytes(zip_listing[fname])
            if fname.startswith(self.engine.artifacts_dir):
                fname = fname[len(self.engine.artifacts_dir) + 1:]
            lines.append(bytestr + " " + fname)
        return "\n".join(lines)


class DatapointSerializer(object):
    def __init__(self, owner):
        """
        :type owner: SignalfxUploader
        """
        super(DatapointSerializer, self).__init__()
        self.owner = owner
        self.multi = 1000  # miltiplier factor for reporting

    def get_kpi_body(self, data_buffer, tags, is_final):
        # - reporting format:
        #   {labels: <data>,    # see below
        #    sourceID: <id of BlazeMeterClient object>,
        #    [is_final: True]}  # for last report
        #
        # - elements of 'data' are described in __get_label()
        #
        # - elements of 'intervals' are described in __get_interval()
        #   every interval contains info about response codes have gotten on it.
        report_items = BetterDict()

        signalfx_labels_list = []

        if data_buffer:
            self.owner.first_ts = min(self.owner.first_ts, data_buffer[0][DataPoint.TIMESTAMP])
            self.owner.last_ts = max(self.owner.last_ts, data_buffer[-1][DataPoint.TIMESTAMP])

            # following data is received in the cumulative way
            # Commented for a while / doctornkz

            for label, kpi_set in iteritems(data_buffer[-1][DataPoint.CUMULATIVE]):
                 report_item = self.__get_label(label, kpi_set)
                 self.__add_errors(report_item, kpi_set)  # 'Errors' tab
                 report_items[label] = report_item

            # fill 'Timeline Report' tab with intervals data
            # intervals are received in the additive way
            for dpoint in data_buffer:
                time_stamp = dpoint[DataPoint.TIMESTAMP]
                for label, kpi_set in iteritems(dpoint[DataPoint.CURRENT]):
                    exc = TaurusInternalException('Cumulative KPISet is non-consistent')
                    report_item = report_items.get(label, exc)
                    report_item['intervals'].append(self.__get_interval(kpi_set, time_stamp))
            
            # TODO: Remove it after implementation
            # metric = {'metric': measurement, 'timestamp': ts * 1000, 'dimensions':dimensions, 'value': value} 

        report_items = [report_items[key] for key in sorted(report_items.keys())]  # convert dict to list
        
        for data_set in report_items:
            intervals = data_set['intervals'] # get intervals for requests
            signalfx_label = []

            for interval in intervals:
                timestamp = interval['ts'] * self.multi
                dimensions = copy.deepcopy(tags)
                dimensions.update({'label': data_set['name']})
                
                # Overall stats : RPS, Threads, procentiles and mix/man/avg
                signalfx_label.extend([
                    {'timestamp': timestamp, 'metric': 'RPS', 'dimensions': dimensions, 'value': interval['n']},
                    {'timestamp': timestamp, 'metric': 'Threads', 'dimensions': dimensions, 'value': interval['na']},
                    {'timestamp': timestamp, 'metric': 'Failures', 'dimensions': dimensions, 'value': interval['ec']},
                    {'timestamp': timestamp, 'metric': 'min', 'dimensions': dimensions, 'value': interval['t']['min']},
                    {'timestamp': timestamp, 'metric': 'p50', 'dimensions': dimensions, 'value': interval['t']['p50']},
                    {'timestamp': timestamp, 'metric': 'p90', 'dimensions': dimensions, 'value': interval['t']['p90']},
                    {'timestamp': timestamp, 'metric': 'p95', 'dimensions': dimensions, 'value': interval['t']['p95']},
                    {'timestamp': timestamp, 'metric': 'p99', 'dimensions': dimensions, 'value': interval['t']['p99']},
                    {'timestamp': timestamp, 'metric': 'p99.9', 'dimensions': dimensions, 'value': interval['t']['p99.9']},
                    {'timestamp': timestamp, 'metric': 'max', 'dimensions': dimensions, 'value': interval['t']['max']},
                    {'timestamp': timestamp, 'metric': 'avg', 'dimensions': dimensions, 'value': interval['t']['avg']}
                    ])
                
                # Detailed info : Error
                for error in interval['rc']:
                    error_dimensions = copy.deepcopy(dimensions)
                    error_dimensions['rc'] = error['rc']
                    signalfx_label.append(
                        {'timestamp': timestamp, 'metric': 'rc', 'dimensions': error_dimensions, 'value': error['n']}
                    )

            signalfx_labels_list.extend(signalfx_label)

        #data = {"gauge": signalfx_list, "sourceID": id(self.owner)}
        data = {"gauge": signalfx_labels_list}
        #print(data)
        return to_json(data)

    @staticmethod
    def __add_errors(report_item, kpi_set):
        errors = kpi_set[KPISet.ERRORS]
        for error in errors:
            if error["type"] == KPISet.ERRTYPE_ERROR:
                report_item['errors'].append({
                    'm': error['msg'],
                    "rc": error['rc'],
                    "count": error['cnt'],
                })
            elif error["type"] == KPISet.ERRTYPE_SUBSAMPLE:
                report_item['failedEmbeddedResources'].append({
                    "count": error['cnt'],
                    "rm": error['msg'],
                    "rc": error['rc'],
                    "url": list(error['urls'])[0] if error['urls'] else None,
                })
            else:
                report_item['assertions'].append({
                    'failureMessage': error['msg'],
                    'name': error['tag'] if error['tag'] else 'All Assertions',
                    'failures': error['cnt']
                    # TODO: "count", "errors" = ? (according do Udi's format description)
                })

    def __get_label(self, name, cumul):
        return {
            "n": cumul[KPISet.SAMPLE_COUNT],  # total count of samples
            "name": name if name else 'ALL',  # label
            "interval": 1,  # not used
            "intervals": [],  # list of intervals, fill later
            "samplesNotCounted": 0,  # not used
            "assertionsNotCounted": 0,  # not used
            "failedEmbeddedResources": [],  # not used
            "failedEmbeddedResourcesSpilloverCount": 0,  # not used
            "otherErrorsCount": 0,  # not used
            "errors": [],  # list of errors, fill later
            "assertions": [],  # list of assertions, fill later
            "percentileHistogram": [],  # not used
            "percentileHistogramLatency": [],  # not used
            "percentileHistogramBytes": [],  # not used
            "empty": False,  # not used
            "summary": self.__get_summary(cumul)  # summary info
        }

    def __get_summary(self, cumul):
        return {
            "first": self.owner.first_ts,
            "last": self.owner.last_ts,
            "duration": self.owner.last_ts - self.owner.first_ts,
            "failed": cumul[KPISet.FAILURES],
            "hits": cumul[KPISet.SAMPLE_COUNT],

            "avg": int(self.multi * cumul[KPISet.AVG_RESP_TIME]),
            "min": int(self.multi * cumul[KPISet.PERCENTILES]["0.0"]) if "0.0" in cumul[KPISet.PERCENTILES] else 0,
            "max": int(self.multi * cumul[KPISet.PERCENTILES]["100.0"]) if "100.0" in cumul[KPISet.PERCENTILES] else 0,
            "std": int(self.multi * cumul[KPISet.STDEV_RESP_TIME]),
            "tp50": int(self.multi * cumul[KPISet.PERCENTILES]["50.0"]) if "50.0" in cumul[KPISet.PERCENTILES] else 0,
            "tp50": int(self.multi * cumul[KPISet.PERCENTILES]["50.0"]) if "50.0" in cumul[KPISet.PERCENTILES] else 0,
            "tp90": int(self.multi * cumul[KPISet.PERCENTILES]["90.0"]) if "90.0" in cumul[KPISet.PERCENTILES] else 0,
            "tp95": int(self.multi * cumul[KPISet.PERCENTILES]["95.0"]) if "95.0" in cumul[KPISet.PERCENTILES] else 0,
            "tp99": int(self.multi * cumul[KPISet.PERCENTILES]["99.0"]) if "99.0" in cumul[KPISet.PERCENTILES] else 0,
            "tp99.9": int(self.multi * cumul[KPISet.PERCENTILES]["99.9"]) if "99.9" in cumul[KPISet.PERCENTILES] else 0,

            "latencyAvg": int(self.multi * cumul[KPISet.AVG_LATENCY]),
            "latencyMax": 0,
            "latencyMin": 0,
            "latencySTD": 0,

            "bytes": cumul[KPISet.BYTE_COUNT],
            "bytesMax": 0,
            "bytesMin": 0,
            "bytesAvg": int(cumul[KPISet.BYTE_COUNT] / float(cumul[KPISet.SAMPLE_COUNT])),
            "bytesSTD": 0,

            "otherErrorsSpillcount": 0,
        }

    def __get_interval(self, item, time_stamp):
        #   rc_list - list of info about response codes:
        #   {'n': <number of code encounters>,
        #    'f': <number of failed request (e.q. important for assertions)>
        #    'rc': <string value of response code>}
        rc_list = []
        for r_code, cnt in iteritems(item[KPISet.RESP_CODES]):
            fails = [err['cnt'] for err in item[KPISet.ERRORS] if str(err['rc']) == r_code]
            rc_list.append({"n": cnt, 'f': fails, "rc": r_code})

        return {
            "ec": item[KPISet.FAILURES],
            "ts": time_stamp,
            "na": item[KPISet.CONCURRENCY],
            "n": item[KPISet.SAMPLE_COUNT],
            "failed": item[KPISet.FAILURES],
            "rc": rc_list,
            "t": {
                "min": int(self.multi * item[KPISet.PERCENTILES]["0.0"]) if "0.0" in item[KPISet.PERCENTILES] else 0,
                "p50": int(self.multi * item[KPISet.PERCENTILES]["50.0"]) if "50.0" in item[KPISet.PERCENTILES] else 0,
                "p90": int(self.multi * item[KPISet.PERCENTILES]["90.0"]) if "90.0" in item[KPISet.PERCENTILES] else 0,
                "p95": int(self.multi * item[KPISet.PERCENTILES]["95.0"]) if "95.0" in item[KPISet.PERCENTILES] else 0,
                "p99": int(self.multi * item[KPISet.PERCENTILES]["99.0"]) if "99.0" in item[KPISet.PERCENTILES] else 0,
                "p99.9": int(self.multi * item[KPISet.PERCENTILES]["99.9"]) if "99.9" in item[KPISet.PERCENTILES] else 0,
                "max": int(self.multi * item[KPISet.PERCENTILES]["100.0"]) if "100.0" in item[KPISet.PERCENTILES] else 0,
                "sum": self.multi * item[KPISet.AVG_RESP_TIME] * item[KPISet.SAMPLE_COUNT],
                "n": item[KPISet.SAMPLE_COUNT],
                "std": self.multi * item[KPISet.STDEV_RESP_TIME],
                "avg": self.multi * item[KPISet.AVG_RESP_TIME]
            },
            "lt": {
                "min": 0,
                "max": 0,
                "sum": self.multi * item[KPISet.AVG_LATENCY] * item[KPISet.SAMPLE_COUNT],
                "n": item[KPISet.SAMPLE_COUNT],
                "std": 0,
                "avg": self.multi * item[KPISet.AVG_LATENCY]
            },
            "by": {
                "min": 0,
                "max": 0,
                "sum": item[KPISet.BYTE_COUNT],
                "n": item[KPISet.SAMPLE_COUNT],
                "std": 0,
                "avg": item[KPISet.BYTE_COUNT] / float(item[KPISet.SAMPLE_COUNT])
            },
        }

