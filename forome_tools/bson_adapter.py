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
import bson

#===============================================
class BsonAdapter:

    sMode = None

    @classmethod
    def _getMode(cls):
        if cls.sMode is None:
            try:
                bson.dumps({"a": 1})
                cls.sMode = 1
            except Exception:
                cls.sMode = 0
        return cls.sMode

    @classmethod
    def encode(cls, obj):
        if not isinstance(obj, dict) or ("" in obj and len(obj) == 1):
            obj = {"": obj}
        if cls._getMode() == 0:
            return bson.encode(obj)
        return bson.dumps(obj)

    @classmethod
    def decode(cls, obj_rep):
        if cls._getMode() == 0:
            obj = bson.decode(obj_rep)
        else:
            obj = bson.loads(obj_rep)
        if isinstance(obj, dict) and ("" in obj and len(obj) == 1):
            return obj[""]
        return obj
