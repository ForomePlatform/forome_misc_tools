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
import os, logging
from shutil import which
from subprocess import Popen, PIPE

#===============================================
class SphinxDocumentationSet:
    def __init__(self, descriptor):
        if not isinstance(descriptor, dict):
            logging.error("Imporper Sphinx document-set "
                "descriptor in configuration")
            assert False
        self.mId = descriptor["id"]
        self.mTitle = descriptor["title"]
        self.mPathUrl = descriptor.get("url")
        self.mTopIndex = descriptor.get("index", "index.html")

        if not self.mPathUrl:
            self.activate(descriptor.get("source"),
                descriptor.get("build"), descriptor.get("path"))
        else:
            logging.info("Sphinx doc set %s refers to %s"
                % (self.mId, self.mPathUrl))

    def isActive(self):
        return self.mPathUrl is not None

    def getTitle(self):
        return self.mTitle

    def getId(self):
        return self.mId

    def getUrl(self, doc_name = None):
        if doc_name is None:
            doc_name = self.mTopIndex
        return self.mPathUrl + doc_name

    def dump(self):
        return {
            "id": self.mId,
            "title": self.mTitle,
            "url": self.mPathUrl}

    def activate(self, path_source, path_build, local_path):
        sphinx_build_exe = os.environ.get("SPHINX_BUILD")
        if sphinx_build_exe is None:
            if which("sphinx-build") is None:
                logging.error("Install sphinx utility sphinx-build\n"
                    "or skip Sphinx documentation generation\n"
                    "or set environment SPHINX_BUILD")
                #  assert False
                return
            sphinx_build_exe = "sphinx-build"
        if (not path_source or not path_build or not local_path):
            logging.error("Improper document-set descriptor "
                "build in configuration")
            #  assert False
            return
        proc = Popen([sphinx_build_exe, "-b", "html", "-a", "-q",
            path_source, path_build],
            stdout = PIPE, stderr = PIPE)
        s_outputs = proc.communicate()
        report = ["Sphinx doc set %s activated with source %s and build %s"
            % (self.mId, path_source, path_build)]
        if s_outputs[0]:
            report.append("<stdout>")
            report.append(str(s_outputs[0], "utf-8"))
        if s_outputs[1]:
            report.append("<stderr>")
            report.append(str(s_outputs[1], "utf-8"))
        if len(report) == 1:
            report.append("<done>")
        logging.info("\n".join(report))
        self.mPathUrl = local_path
