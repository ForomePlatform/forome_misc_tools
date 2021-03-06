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

import traceback, logging
from io import StringIO

#========================================
def logException(message, error_mode = True, limit_stack = 20):
    rep = StringIO()
    traceback.print_exc(file = rep, limit = limit_stack)
    if error_mode:
        logging.error(message + "\n" + rep.getvalue())
    else:
        logging.warning(message + "\n" + rep.getvalue())
    return rep.getvalue()
