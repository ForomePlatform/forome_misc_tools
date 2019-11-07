import gzip
import os
import sys
import json

def read_json(path):
    variants = dict()
    with gzip.open(path) as f:
        while True:
            line = f.readline()
            if (not line):
                break
            data = json.loads(line)
            record_type = data["record_type"]
            if (record_type != "variant"):
                continue

            chromosome = data["_filters"]["chromosome"][3:]
            pos = data["data"]["start"]
            if not chromosome in variants:
                variants[chromosome] = set()
            variants[chromosome].add(pos)

    return variants


def scan_vcf(path_to_vcf, variants, output_vcf):
    with open(path_to_vcf) as f, open(output_vcf, "w") as target:
        while True:
            line = f.readline()
            if (not line):
                break
            if (line.startswith('#')):
                target.write(line)
                continue

            x = line[:100].split('\t')
            chromosome = x[0]
            pos = int(x[1])

            if chromosome in variants:
                if pos in variants[chromosome]:
                    target.write(line )


if __name__ == '__main__':
    source = "/Users/misha/projects/bgm/cases/pf0003_wes/pf0003_wes.vcf_utils"
    subset = "/Users/misha/projects/bgm/cases/pf0003_wes/test_anfisa.json.gz"
    target = "/Users/misha/projects/bgm/cases/pf0003_wes/test.vcf_utils"

    variants = read_json(subset)
    scan_vcf(source, variants, target)
