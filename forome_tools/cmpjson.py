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

import sys, os, json
from hashlib import md5
from argparse import ArgumentParser
from collections import Counter
from difflib import Differ

from .read_json import JsonLineReader
from .diff_smpjson import diffSamples
#=====================================
class PrimaryKeyHandler:
    def __init__(self, primary_key = None, ignore_case = False):
        self.mInstruction = primary_key
        self.mIgnoreCase = ignore_case
        self.mPrimKey = None
        self.mPrimKeySeq = None
        self.mIsSet = False

    def isOK(self):
        return self.mInstruction is not None

    @staticmethod
    def _prepKey(key, record, map_fields):
        if map_fields:
            assert key.lower() in map_fields, (
                f"No such key '{key}' in record, up to ignore case")
            return map_fields[key]
        assert key in record, f"No such key '{key}' in record"
        return key

    def _setup(self, record):
        self.mIsSet = True
        if self.mInstruction is None:
            return
        map_fields = None
        if self.mIgnoreCase:
            map_fields = {field.lower(): field
                for field in record.keys()}
        if ',' in self.mInstruction:
            self.mPrimKeySeq = [self._prepKey(key, record, map_fields)
                for key in self.mInstruction.split(',')]
            print("Use composite key: " + ",".join(self.mPrimKeySeq),
                file = sys.stderr)
        else:
            self.mPrimKey = self._prepKey(
                self.mInstruction, record, map_fields)
            print("Use primary key: " + self.mPrimKey, file = sys.stderr)

    def apply(self, record):
        if (not self.mIsSet):
            self._setup(record)
        if self.mPrimKey is not None:
            return str(record[self.mPrimKey])
        return "|".join([str(record[key]) for key in self.mPrimKeySeq])

#=====================================
class _SampleCollector:
    sSampleSize = 5

    def __init__(self, file_name, prim_key_h, use_keystat):
        self.mSamples = None
        self.mCntTotal = 0
        self.mCntFiltered = 0
        self.mFileName = file_name
        self.mPrimKeyH = prim_key_h
        self.mFieldStat = Counter() if use_keystat else None

    def __len__(self):
        return len(self.mSamples)

    def getFileName(self):
        return self.mFileName

    def getPrimaryValue(self, idx):
        return self.mSamples[idx][0]

    def getRecord(self, idx):
        return self.mSamples[idx][1]

    def getPrimaryValues(self):
        return [smp_info[0] for smp_info in self.mSamples]

    def getCntTotal(self):
        return self.mCntTotal

    def getCntFiltered(self):
        return self.mCntFiltered

    def getFieldStat(self):
        return self.mFieldStat

    def getSamples(self):
        return [smp_info[1] for smp_info in self.mSamples]

    def readAll(self, filter_func = None):
        assert self.mCntTotal == 0, "Use readAll only once!"
        with JsonLineReader(self.mFileName) as input:
            for line_no, record in enumerate(input):
                self.mCntTotal += 1
                if filter_func is not None and filter_func(record):
                    self.mCntFiltered += 1
                    continue
                if self.mPrimKeyH.isOK():
                    prim_v = self.mPrimKeyH.apply(record)
                else:
                    prim_v = line_no
                self.regOne(prim_v, record)
                if self.mFieldStat is not None:
                    for key, val in record.items():
                        if val is not None:
                            self.mFieldStat[key] += 1
        self.finishUp()

    def report(self):
        ret = self.mFileName + " | Kept: %d/%d" % (len(self), self.mCntTotal)
        if self.mCntFiltered > 0:
            ret += " filtered: %d" % self.mCntFiltered
        return ret

#=====================================
class HashSampleCollector(_SampleCollector):
    sHashPrefix = ""

    @classmethod
    def makeHash(cls, value):
        return md5((cls.sHashPrefix + str(value)).encode(
            encoding="utf-8")).digest()

    def __init__(self, file_name, prim_key_h, use_keystat):
        _SampleCollector.__init__(self, file_name, prim_key_h, use_keystat)
        self.mSamples = []
        self.mMaxHash = None

    def _flush(self):
        if len(self.mSamples) < self.sSampleSize:
            return
        self.mSamples.sort(key = lambda info: info[2])
        self.mSamples = self.mSamples[:self.sSampleSize]
        self.mMaxHash = self.mSamples[-1][2]

    def regOne(self, primary_value, record):
        hash = self.makeHash(primary_value)
        if len(self.mSamples) < self.sSampleSize:
            self.mSamples.append((primary_value, record, hash))
            return
        if (self.mMaxHash is None
                or len(self.mSamples) > 10 * self.sSampleSize):
            self._flush()
        if hash <= self.mMaxHash:
            self.mSamples.append((primary_value, record, hash))

    def finishUp(self):
        self._flush()
        self.mMaxHash = False

#=====================================
class ExpectSampleCollector(_SampleCollector):
    def __init__(self, file_name, prim_key_h, use_keystat, primary_values):
        _SampleCollector.__init__(self, file_name, prim_key_h, use_keystat)
        self.mSamples = [[prim_v, None] for prim_v in primary_values]
        self.mDict = {prim_v: idx
            for idx, prim_v in enumerate(primary_values)}
        self.mCntRegistered = 0
        self.mCntKept = 0
        self.mCntConflict = 0

    def regOne(self, primary_value, record):
        idx = self.mDict.get(primary_value)
        if idx is None:
            return
        if self.mSamples[idx][1] is not None:
            self.mCntConflict += 1
            print("Conflict by primary values: " + str(primary_value),
                file = sys.stderr)
        else:
            self.mCntKept += 1
            self.mSamples[idx][1] = record

    def finishUp(self):
        pass

    def report(self):
        cnt_not_null = sum(smp_info[1] is not None
            for smp_info in self.mSamples)
        ret = self.getFileName() + " | Kept: %d/%d/%d" % (
            cnt_not_null, self.mCntKept, self.getCntTotal())
        if self.mCntConflict > 0:
            ret += " conflicts: %d" % self.mCntConflict
        if self.getCntFiltered() > 0:
            ret += " filtered: %d" % self.getCntFiltered()
        return ret

#=====================================
def recRepr(record):
    return json.dumps(record, sort_keys = True, indent = 4)

#=====================================
class DiffHandler:
    def __init__(self, collector1, collector2,
            cmp_ign_fields, cmp_ign_values,
            force_mode, deep_mode, opt_smpdir12):
        self.mRecords = []
        self.mFields = None
        self.mFieldsDiff = None
        self.mCollectors = [collector1, collector2]
        self.mIgnFields = cmp_ign_fields
        self.mIgnValues = cmp_ign_values
        self.mForceMode = force_mode
        self.mDeepDiffMode = deep_mode
        self.mSmpDir12 = opt_smpdir12
        self.mDiffReport = None
        for idx in range(len(collector1)):
            prim_v = collector1.getPrimaryValue(idx)
            rec1 = collector1.getRecord(idx)
            rec2 = collector2.getRecord(idx)
            self.mRecords.append([self.compare(rec1, rec2),
                recRepr(rec1), recRepr(rec2), prim_v])
        self.mBadCnt = sum(not rec_info[0] for rec_info in self.mRecords)

        fld_cnt1 = collector1.getFieldStat()
        fld_cnt2 = collector2.getFieldStat()
        if fld_cnt1 is None:
            return
        fields1 = set(fld_cnt1.keys())
        fields2 = set(fld_cnt2.keys())
        self.mRest1 = sorted(fields1 - fields2)
        self.mRest2 = sorted(fields2 - fields1)
        self.mFields = []
        for fld in sorted(fields1 | fields2):
            self.mFields.append((fld,
                fld_cnt1.get(fld, 0), fld_cnt2.get(fld, 0)))
        self.mFieldsDiff = (len(self.mRest1) + len(self.mRest2) > 0)

    def compare(self, rec1, rec2):
        rec1 = rec1.copy()
        if rec2 is None:
            return False
        rec2 = rec2.copy()
        for rec in (rec1, rec2):
            for fld in self.mIgnFields:
                if fld in rec:
                    del rec[fld]
            for fld in self.mIgnValues:
                if fld in rec:
                    rec[fld] = None
        return recRepr(rec1) == recRepr(rec2)

    def getStatus(self):
        if self.mBadCnt > 0:
            return "DIFFERENCE IN SAMPLES"
        if self.mFieldsDiff:
            return "DIFFERENCE IN KEYS"
        return "LOOKS SAME"

    def reportHead(self, output):
        print("Comparison %s vs %s" % tuple(collector.getFileName()
            for collector in self.mCollectors), file = output)
        print("Status: " + self.getStatus(), file = output)
        if self.mFieldsDiff is None:
            return
        if not self.mFieldsDiff and not self.mForceMode:
            print("(-) No essential differences in fields", file = output)
        else:
            if len(self.mRest1) > 0:
                print("(!) Extra fields in file 1: " + " ".join(self.mRest1),
                    file = output)
            if len(self.mRest2) > 0:
                print("(!) Extra fields in file 2: " + " ".join(self.mRest2),
                    file = output)
            print("Fields statistic:", file = output)
            for field_info in self.mFields:
                print("\t" + "\t".join(map(str, field_info)), file = output)
            print("===", file = output)
        if self.mBadCnt == 0:
            print("(-) No difference in %d samples" % len(self.mRecords),
                file = output)
        else:
            print("(!) Difference in %d/%d samples"
                % (self.mBadCnt, len(self.mRecords)), file = output)
        if self.mDeepDiffMode and (self.mBadCnt > 0 or self.mForceMode):
            print(file = output)
            self.mDiffReport = diffSamples(self.mCollectors[0].getSamples(),
                self.mCollectors[1].getSamples(), output)
            print(file = output)

    def reportInStd(self):
        self.reportHead(sys.stdout)
        if self.mBadCnt == 0:
            return
        diff = Differ()
        cnt = 0
        for q_same, rec_repr1, rec_repr2, prim_v in self.mRecords:
            cnt += 1
            if q_same:
                continue
            print("==========Cmp item %s==(%d/%d)============="
                % (str(prim_v), cnt, len(self.mRecords)))
            cur_diff = False
            prev_line_ok = None
            for line in diff.compare(
                    rec_repr1.splitlines(), rec_repr2.splitlines()):
                if line.startswith(' '):
                    if not cur_diff:
                        prev_line_ok = line
                    else:
                        assert prev_line_ok is None
                        print(line)
                        cur_diff = False
                    continue
                if prev_line_ok is not None:
                    print(prev_line_ok)
                    prev_line_ok = None
                cur_diff = True
                print(line)
        print("=============Done=====================")

    def reportInDir(self, outdir, title):
        if not os.path.exists(outdir):
            print("Creation report directory: " + outdir)
            os.mkdir(outdir)
        with open(outdir + ("/%s.log" % title),
                "w", encoding = "utf-8") as output:
            self.reportHead(output)
        print("Comparison %s vs %s" % tuple(collector.getFileName()
            for collector in self.mCollectors))
        print("Status: " + self.getStatus())
        if self.mDiffReport:
            print("Difference fields in samples report:", self.mDiffReport)
        print("Report is stored to %s/%s.log" % (outdir, title))
        if self.mBadCnt == 0 and not self.mForceMode:
            return
        outnames = []
        for idx in (1, 2):
            if self.mSmpDir12:
                outsubdir = outdir + ("/%d" % idx)
                outname = outsubdir + ("/%s.rep" % title)
                if not os.path.exists(outsubdir):
                    os.mkdir(outsubdir)
            else:
                outname = outdir + ("/%s-%d.rep" % (title, idx))
            outnames.append(outname)

            with open(outname, "w", encoding = "utf-8") as output:
                print("===Difference in %d/%d samples==="
                    % (self.mBadCnt, len(self.mRecords)), file = output)
                cnt = 0
                for rec_info in self.mRecords:
                    cnt += 1
                    if rec_info[0] and not self.mForceMode:
                        continue
                    print("==========item %s====(%d/%d)==========="
                        % (str(rec_info[-1]), cnt, len(self.mRecords)),
                        file = output)
                    print(rec_info[idx], file = output)
        print("Sample files are stored: " + " ".join(outnames))

#=====================================
if __name__ == "__main__":
    parser = ArgumentParser()
    parser.add_argument("--hashpref", default = "",
        help = "Prefix used for random hash generation")
    parser.add_argument("-n", "--number", type = int, default = 5,
        help = "Number of samples")
    parser.add_argument("-s", "--seq",
        help = "Direct list of records, comma separated")
    parser.add_argument("-p", "--primarykey",
        help = "Primary key in records, comma-separated if multiple")
    parser.add_argument("-o", "--outdir",
        help = "Directory to keep result of comparison")
    parser.add_argument("-t", "--title", default = "samples",
        help = "Name of report files with samples, actual with --outdir")
    parser.add_argument("-k", "--keys_stat", action="store_true",
        help = "Collect statistics for record fields")
    parser.add_argument("-f", "--force", action="store_true",
        help = "Make full report, even everything is OK")
    parser.add_argument("-d", "--deepdiff", action="store_true",
        help = "Make deep difference report")
    parser.add_argument("-P", "--primaryignorecase", action="store_true",
        help = "Ignore case in primary key")
    parser.add_argument("--smpdir12", action="store_true",
        help = "In output samples: use subdirectories 1 and 2")
    parser.add_argument("--filter",
        help = "Filter records: <name> or <name>=<value>")
    parser.add_argument("--cmp_ign_fields", default = "",
        help = "Fields to ignore in comparation, comma separated")
    parser.add_argument("--cmp_ign_values", default = "",
        help = "Fields to ignore values in comparation, comma-separated")
    parser.add_argument("source", nargs = 2, help = "Two js files to compare")
    run_args = parser.parse_args()

    _SampleCollector.sSampleSize = run_args.number
    HashSampleCollector.sHashPrefix = run_args.hashpref

    filter_func = None
    if run_args.filter:
        key, _, val = run_args.filter.partition('=')
        if val is None:
            filter_func = lambda record: record.get(key) is not None
            print("Use presence of %s as filtation" % key, file = sys.stderr)
        else:
            filter_func = lambda record: str(record.get(key)) == val
            print("Use %s=%s as filtation" % (key, val), file = sys.stderr)

    prim_key_instr = run_args.primarykey
    if '|' in prim_key_instr:
        prim_key1, prim_key2 = prim_key_instr.split('|')
    else:
        prim_key1 = prim_key2 = prim_key_instr

    prim_key1_h = PrimaryKeyHandler(prim_key1, run_args.primaryignorecase)
    if run_args.seq:
        collector1 = ExpectSampleCollector(
            run_args.source[0], prim_key1_h, run_args.keys_stat,
            map(int(run_args.seq.split(','))))
    else:
        collector1 = HashSampleCollector(
            run_args.source[0], prim_key1_h, run_args.keys_stat)
    collector1.readAll(filter_func)
    print("File 1: " + collector1.report(), file = sys.stderr)

    prim_key2_h = PrimaryKeyHandler(
        prim_key2, run_args.primaryignorecase)
    collector2 = ExpectSampleCollector(run_args.source[1], prim_key2_h,
        run_args.keys_stat, collector1.getPrimaryValues())
    collector2.readAll()
    print("File 2: " + collector2.report(), file = sys.stderr)

    diff_h = DiffHandler(collector1, collector2,
        run_args.cmp_ign_fields.split(','),
        run_args.cmp_ign_values.split(','),
        run_args.force, run_args.deepdiff, run_args.smpdir12)
    if run_args.outdir:
        diff_h.reportInDir(run_args.outdir, run_args.title)
    else:
        diff_h.reportInStd(run_args)
