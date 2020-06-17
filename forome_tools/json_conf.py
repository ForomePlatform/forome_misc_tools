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

import os, json, re

#========================================
sCommentLinePatt = re.compile(r"^\s*//.*$")

def readCommentedJSon(fname):
    lines = []
    with open(fname, "r", encoding = "utf-8") as inp:
        for line in inp:
            if not sCommentLinePatt.match(line):
                lines.append(line.rstrip())
            else:
                lines.append("")
    return "\n".join(lines)

#========================================
def loadCommentedJSon(fname):
    return json.loads(readCommentedJSon(fname))

#========================================
def loadJSonConfig(config_file, meta_variables = None,
        home_base_file = None, home_base_level = 0):
    content = readCommentedJSon(config_file)
    used_variables = set()
    if home_base_file is not None:
        home_path = os.path.abspath(home_base_file)
        for _ in range(home_base_level + 1):
            home_path = os.path.dirname(home_path)
        content = content.replace('${HOME}', home_path)
        used_variables.add("HOME")
    if meta_variables is not None:
        for meta_name, meta_val in meta_variables:
            assert meta_name not in used_variables, (
                "Double meta variable name: %s" % meta_name)
            content = content.replace('${%s}' % meta_name, meta_val)
            used_variables.add(meta_name)
    pre_config = json.loads(content)

    file_path_def = pre_config.get("file-path-def")
    if file_path_def:
        for key, value in file_path_def.items():
            assert key not in used_variables, (
                "Meta variable %s duplication" % key)
            content = content.replace('${%s}' % key, value)
    return json.loads(content)
