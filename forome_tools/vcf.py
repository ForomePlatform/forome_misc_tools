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

import gzip, bz2
from glob import glob
#=====================================
class VCF_Support:
    def __init__(self, select_fields = None, multi_fields = None,
            ignore_fields = None, ignore_dup = False):
        self.mSelectFields = set(select_fields) if select_fields else None
        self.mMultiFields = set(multi_fields) if multi_fields else set()
        self.mIgnoreFields = set(ignore_fields) if ignore_fields else set()
        self.mIgnoreDup = ignore_dup

    def parse(self, line):
        if (line[0] == '#'):
            return None
        fields = [fld.strip() for fld in line.split('\t')]
        if len(fields[0]) > 4:
            # Not a chromosome
            return None
        assert len(fields) == 9
        rec = {
            "chrom": fields[0],
            "source": fields[1],
            "feature": fields[2],
            "p_start": int(fields[3]),
            "p_end": int(fields[4]),
            "score": fields[5],
            "strand": fields[6],
            "frame": fields[7]}
        for pair in fields[8].split(';'):
            pair = pair.strip();
            if not pair:
                continue
            key, _, val = pair.partition(' ')
            if val is None:
                val = True
            else:
                val = val.strip('"')
            if not key:
                assert val is None, f"Missed pair {pair}"
                continue
            if (self.mSelectFields is not None
                    and key not in self.mSelectFields):
                continue
            if key in self.mIgnoreFields:
                continue
            if key in self.mMultiFields:
                if key in rec:
                    rec[key].append(val)
                else:
                    rec[key] = [val]
            else:
                if key in rec:
                    assert not self.mIgnoreDup, f"Key duplication {key}"
                else:
                    rec[key] = val
        return rec

    #========================================
    def readFile(self, src, transform_f = None):
        if '*' in src:
            names = sorted(glob(src))
        else:
            names = [src]
        if transform_f is None:
            process_f = self.parse
        else:
            def process_f(line):
                return transform_f(self.parse(line))
        for nm in names:
            if nm.endswith('.gz'):
                with gzip.open(nm, 'rt', encoding = "utf-8") as inp:
                    for line in inp:
                        rec = process_f(line)
                        if rec is not None:
                            yield rec
            elif nm.endswith('.bz2'):
                with bz2.BZ2File(nm, 'rt', encoding = "utf-8") as inp:
                    for line in inp:
                        rec = process_f(line)
                        if rec is not None:
                            yield rec
            else:
                with open(nm, 'r', encoding = 'utf-8') as inp:
                    for line in inp:
                        rec = process_f(line)
                        if rec is not None:
                            yield rec
