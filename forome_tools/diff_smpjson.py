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
import sys, json
from collections import defaultdict

MARK_LEFT = "<--<--"
MARK_RIGHT = "-->-->"
#=====================================
def diffSamples(all_samples1, all_samples2, out, details = True):
    samples1, samples2 = [], []
    cnt_nulls = 0
    for smp1, smp2 in zip(all_samples1, all_samples2):
        assert smp1 is not None, "left samples must nby not null!"
        if smp2 is None:
            cnt_nulls += 1
        else:
            samples1.append(_normalizeSample(smp1))
            samples2.append(_normalizeSample(smp2))
    diff_status = []

    fields1, fields2 = set(), set()
    for smp1, smp2 in zip(samples1, samples2):
        fields1 |= set(smp1.keys())
        fields2 |= set(smp2.keys())
    status_seq = defaultdict(list)
    for field in sorted(fields1 | fields2):
        status_seq[diffStatus(field, samples1, samples2)].append(field)
    for status in ("DIFF", "NEW-LEFT", "NEW-RIGHT", "SAME"):
        if status in status_seq:
            diff_status.append("%s:%d" % (status, len(status_seq[status])))
    if cnt_nulls > 0:
        diff_status.append("|null right sample:%d" % cnt_nulls)
    full_status = " + ".join(diff_status)
    print("=FIELDS-DIFF-STATUS:", full_status, file = out)
    for status in ("DIFF", "NEW-LEFT", "NEW-RIGHT"):
        if status not in status_seq:
            continue
        print("\t\t", status + " fields: ", " ".join(status_seq[status]), file = out)

    if details:
        print("=====Details====", file = out)
        if "DIFF" in status_seq:
            print ("=Report DIFF fields (%d)"
                % (len(status_seq["DIFF"])), file = out)
            for field in status_seq["DIFF"]:
                reportDiffField(field, samples1, samples2, out)

        if "NEW-LEFT" in status_seq:
            print ("=Report NEW-LEFT fields (%d)"
                % (len(status_seq["NEW-LEFT"])), file = out)
            for field in status_seq["NEW-LEFT"]:
                reportNewField(field, MARK_LEFT, samples1, out)

        if "NEW-RIGHT" in status_seq:
            print ("=Report NEW-RIGHT fields (%d)"
                % (len(status_seq["NEW-RIGHT"])), file = out)
            for field in status_seq["NEW-RIGHT"]:
                reportNewField(field, MARK_RIGHT, samples2, out)
    print("====End or difference report")
    return full_status

#=====================================
def _normalizeSample(sample):
    if None not in sample.values():
        return sample
    ret = dict()
    for key, val in sample.items:
        if val is not None:
            ret[key] = val
    return ret

#=====================================
def diffStatus(field, samples1, samples2):
    cnt_diff, left_null, right_null = 0, True, True
    for smp1, smp2 in zip(samples1, samples2):
        val1 = json.dumps(smp1.get(field))
        val2 = json.dumps(smp2.get(field))
        left_null &= (val1 == "null")
        right_null &= (val2 == "null")
        if (val1 != val2):
            cnt_diff += 1
    if left_null:
        assert not right_null, "Tech: got a bug!"
        return "NEW-RIGHT"
    if right_null:
        return "NEW-LEFT"
    if cnt_diff > 0:
        return "DIFF"
    return "SAME"

#=====================================
def reportDiffField(field, samples1, samples2, out):
    values1, values2 = [], []
    cnt_same = 0
    cnt_null = 0
    for smp1, smp2 in zip(samples1, samples2):
        val1 = json.dumps(smp1.get(field))
        val2 = json.dumps(smp2.get(field))
        if (val1 == val2):
            cnt_same += 1
        elif val2 == "null":
            cnt_null +=1
        else:
            values1.append(val1)
            values2.append(val2)
    print("====================", file = out)
    print("***", field, file = out)
    if cnt_null > 0:
        print("* null values:", cnt_null, file = out)
    if cnt_same > 0:
        print("* same values:", cnt_same, file = out)
    for val1, val2 in zip(values1, values2):
        print(MARK_LEFT, val1, file = out)
        print(MARK_RIGHT, val2, file = out)
        print(file = out)

#=====================================
def reportNewField(field, mark, samples, out):
    values = []
    cnt_null = 0
    for smp in samples:
        val = json.dumps(smp.get(field))
        if val == "null":
            cnt_null += 1
        else:
            values.append(val)
    print("====================", file = out)
    print("***", field, ":", file = out)
    if cnt_null > 0:
        print("* null values:", cnt_null, file = out)
    if len(set(values)) == 1:
        print(mark, "* single value:", len(values),
            "=", values[0], file = out)
        return
    for val in values:
        print(mark, "\t", val, file = out)

#=====================================
if __name__ == "__main__":
    assert len(sys.argv) == 3, ("Usage: file1.rep file2.rep")
    samples_seq = []
    for fname in sys.argv[1:]:
        with open(fname, "r", encoding="utf-8") as inp:
            samples = []
            cur_block = []
            for line in inp:
                if line.startswith("==="):
                    if len(cur_block) > 0:
                        samples.append(json.loads('\n'.join(cur_block)))
                        cur_block = []
                else:
                    cur_block.append(line.rstrip())
            if len(cur_block) > 0:
                samples.append(json.loads('\n'.join(cur_block)))
        samples_seq.append(samples)
    assert len(samples_seq[0]) == len(samples_seq[1]), (
        "Difference in sample counts: %d/%d"
        % (len(samples_seq[0]), len(samples_seq[1])))
    diffSamples(samples_seq[0], samples_seq[1], sys.stdout)
