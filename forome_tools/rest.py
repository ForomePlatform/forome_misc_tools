#  Copyright (c) 2019. Partners HealthCare and other members of
#  Forome Association
#
#  Developed by Sergey Trifonov based on contributions by Joel Krier,
#  Michael Bouzinier, Shamil Sunyaev and other members of Division of
#  Genetics, Brigham and Women's Hospital
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#        http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
#

import json, logging
from urllib.parse import urlsplit, quote
from http.client import HTTPConnection, HTTPSConnection
from .bson_adapter import BsonAdapter
#==================================
class RestAgent:
    sHeadersTab = {
        "json": {
            "Content-Type": "application/json",
            "Encoding": "utf-8"},
        "www": {
            "Content-Type": "application/x-www-form-urlencoded",
            "Encoding": "utf-8"},
    }

    def __init__(self, url, name = None, header_type = "json",
            calm_mode = False):
        url_info = urlsplit(url)
        self.mScheme = url_info.scheme
        assert url_info.scheme in ("http", "https")
        self.mHost = url_info.hostname
        self.mPort = url_info.port
        self.mPath = url_info.path
        self.mHeaderType = header_type
        self.mHeaders = self.sHeadersTab[header_type]
        if self.mPort is None:
            self.mPort = 80
        self.mName = name if name else url
        self.mCalmMode = calm_mode

    def _reportCall(self, method, res):
        logging.info("REST " + method  + " call: " + self.mName + " "
            " response: " + str(res.status) + " reason: " + str(res.reason))

    def call(self, request_data, method = "POST",
            add_path = "", json_rq_mode = True, calm_mode = False,
            plain_return = False):
        if request_data is not None:
            if self.mHeaderType == "www":
                assert isinstance(request_data, dict)
                content = "&".join("%s=%s" % (key, quote(str(val)))
                    for key, val in request_data.items())
            elif json_rq_mode:
                content = json.dumps(request_data, ensure_ascii = False)
            else:
                content = request_data
        else:
            content = ""

        if self.mScheme == "http":
            conn = HTTPConnection(self.mHost, self.mPort)
        else:
            conn = HTTPSConnection(self.mHost, self.mPort)

        rq_path = self.mPath + add_path
        conn.request(method, rq_path,
            body = content.encode("utf-8"), headers = self.mHeaders)
        res = conn.getresponse()
        bson_mode = res.getheader("Content-Type") == "application/bson"
        try:
            content = res.read()
            if not calm_mode and not self.mCalmMode:
                self._reportCall(method, res)
            if res.status != 200:
                if calm_mode:
                    self._reportCall(method, res)
                raise RuntimeError(("Rest call failure (%r):\n" % res.status)
                    + str(content, "utf-8") + '\n========')
        finally:
            res.close()
            del conn
        if plain_return:
            return content
        if method == "DELETE":
            return None
        if bson_mode:
            return BsonAdapter.decode(content)
        return json.loads(str(content, 'utf-8'))
