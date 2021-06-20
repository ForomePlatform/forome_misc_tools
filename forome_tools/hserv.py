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

import os, logging, json
from urllib.parse import parse_qs
from cgi import parse_header, parse_multipart
import logging.config

from .log_err import logException
#========================================
class HServResponse:
    #========================================
    sContentTypes = {
        "css":    "text/css",
        "html":   "text/html",
        "js":     "application/javascript",
        "json":   "application/json",
        "png":    "image/png",
        "txt":    "text/plain",
        "csv":    "text/csv",
        "xlsx":   (
            "application/application/"
            "vnd.openxmlformats-officedocument.spreadsheetml.sheet"),
        "xml":    "text/xml",

        # Standard file format list
        "doc":    "application/msword",
        "docx":   "application/"
            "vnd.openxmlformats-officedocument.wordprocessingml.document",
        "epub":   "application/epub+zip",
        "gz":     "application/gzip",
        "gif":    "image/gif",
        "ico":    "image/vnd.microsoft.icon",
        "jar":    "application/java-archive",
        "jpg":    "image/jpeg",
        "mp3":    "audio/mpeg",
        "mpg":    "video/mpeg",
        "odp":    "application/vnd.oasis.opendocument.presentation",
        "ods":    "application/vnd.oasis.opendocument.spreadsheet",
        "odt":    "application/vnd.oasis.opendocument.text",
        "pdf":    "application/pdf",
        "ppt":    "application/vnd.ms-powerpoint",
        "pptx":   "application/"
            "vnd.openxmlformats-officedocument.presentationml.presentation",
        "rar":    "application/vnd.rar",
        "rtf":    "application/rtf",
        "svg":    "image/svg+xml",
        "tar":    "application/x-tar",
        "wav":    "audio/wav",
        "xls":    "application/vnd.ms-excel",
        "zip":    "application/zip"
    }

    sErrorCodes = {
        202: "202 Accepted",
        204: "204 No Content",
        303: "303 See Other",
        400: "400 Bad Request",
        403: "403 Forbidden",
        408: "408 Request Timeout",
        404: "404 Not Found",
        422: "422 Unprocessable Entity",
        423: "423 Locked",
        500: "500 Internal Error"}

    def __init__(self, start_response):
        self.mStartResponse = start_response

    def makeResponse(self, mode = "html", content = None, error = None,
            add_headers = None, without_decoding = False):
        response_status = "200 OK"
        if error is not None:
            response_status = self.sErrorCodes[error]
        if content is not None:
            if without_decoding:
                response_body = bytes(content)
            else:
                response_body = content.encode("utf-8")
            response_headers = [("Content-Type", self.sContentTypes[mode]),
                                ("Content-Length", str(len(response_body)))]
        else:
            response_body = response_status.encode("utf-8")
            response_headers = []
        if add_headers is not None:
            response_headers += add_headers
        self.mStartResponse(response_status, response_headers)
        return [response_body]

#========================================
class HServHandler:
    sInstance = None

    @classmethod
    def init(cls, application, config, in_container):
        cls.sInstance = cls(application, config, in_container)

    @classmethod
    def request(cls, environ, start_response):
        return cls.sInstance.processRq(environ, start_response)

    def __init__(self, application, config, in_container):
        self.mApplication = application
        self.mDirFiles = config["dir-files"]
        self.mHtmlBase = (config["html-base"]
            if in_container else None)
        if self.mHtmlBase and self.mHtmlBase.endswith('/'):
            self.mHtmlBase = self.mHtmlBase[:-1]
        self.mApplication.setup(config, in_container)

    def checkFilePath(self, path):
        alt_path = self.mApplication.checkFilePath(path)
        if alt_path is not None:
            return alt_path
        for path_from, path_to in self.mDirFiles:
            if path.startswith(path_from):
                return path_to + path[len(path_from):]
        return None

    #===============================================
    def parseRequest(self, environ):
        rq_path = environ["PATH_INFO"]
        if self.mHtmlBase and rq_path.startswith(self.mHtmlBase):
            rq_path = rq_path[len(self.mHtmlBase):]
        if not rq_path:
            rq_path = "/"
        query_string = environ["QUERY_STRING"]

        query_args = dict()
        if query_string:
            for a, v in parse_qs(query_string).items():
                query_args[a] = v[0]

        if environ["REQUEST_METHOD"] == "POST":
            try:
                content_type = environ.get('CONTENT_TYPE')
                pdict = None
                if content_type:
                    content_type, pdict = parse_header(content_type)
                if not content_type:
                    content_type = 'application/x-www-form-urlencoded'
                if content_type == 'multipart/form-data':
                    for a, v in parse_multipart(
                            environ['wsgi.input'], pdict).items():
                        query_args[a] = v[0]
                elif content_type in {'application/x-www-form-urlencoded',
                        'application/json'}:
                    rq_body_size = int(environ.get('CONTENT_LENGTH', 0))
                    rq_body = environ['wsgi.input'].read(
                        rq_body_size).decode("utf-8")
                    if content_type == 'application/json':
                        query_args["@request"] = json.loads(rq_body)
                    else:
                        pdict = parse_qs(rq_body)
                        for a, v in pdict.items():
                            query_args[a] = v[0]
                else:
                    logging.error("Bad content type for POST: " + content_type)
            except Exception:
                logException("Exception on read request body")

        return rq_path, query_args

    #===============================================
    def fileResponse(self, resp_h, fpath,
            query_args, without_decoding):
        if not os.path.exists(fpath):
            return False
        if without_decoding:
            inp = open(fpath, "rb")
            content = inp.read()
        else:
            with open(fpath, "r", encoding = "utf-8") as inp:
                content = inp.read()
        inp.close()

        file_ext  = fpath.rpartition('.')[2]
        add_headers = None

        if file_ext == ".xslx":
            add_headers = [("content-disposition",
                "attachment; filename=%s" %
                query_args.get("disp", fpath.rpartition('/')[2]))]

        return resp_h.makeResponse(mode = file_ext,
            content = content, add_headers = add_headers,
            without_decoding = without_decoding)

    #===============================================
    def _makeResponceException(self, rq_descr, resp_h,
            assertion_text = None):
        msg = "Exception on evaluation"
        error_code = 500
        if assertion_text and not assertion_text.startswith('!'):
            msg = "Improper call"
            error_code = 403
        if assertion_text:
            msg += "\n Error: " + assertion_text
        if rq_descr:
            msg += "\n In context: " + " ".join(rq_descr)
        rep_exc = logException(msg)
        return resp_h.makeResponse(mode = "txt",
            error = error_code, content = msg + "\n" + rep_exc)

    #===============================================
    def processRq(self, environ, start_response):
        resp_h = HServResponse(start_response)
        rq_descr = []
        try:
            rq_path, query_args = self.parseRequest(environ)
            file_path = self.checkFilePath(rq_path)
            if file_path is not None:
                ret = self.fileResponse(resp_h,
                    file_path, query_args, True)
                if ret is not False:
                    return ret
            return self.mApplication.request(
                resp_h, rq_path, query_args, rq_descr)
        except AssertionError as exc:
            return self._makeResponceException(rq_descr, resp_h,
                exc.args[0] if len(exc.args) > 0 else None)
        except Exception:
            return self._makeResponceException(rq_descr, resp_h)

#========================================
def setupHServer(application, config, in_container):
    logging_config = config.get("logging")
    if logging_config:
        logging.config.dictConfig(logging_config)
        logging.basicConfig(level = 0)
    HServHandler.init(application, config, in_container)
    if not in_container:
        return (config["host"], int(config["port"]))
    return None
