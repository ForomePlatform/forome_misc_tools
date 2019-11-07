import os
import sys
from collections import OrderedDict

import vcf as pyvcf
from db.annotations.spliceai import SpliceAI

def execute(cmd):
    print (cmd)
    os.system(cmd)


def call(vcf_file, call_file):
    vcf_reader = pyvcf.Reader(filename=vcf_file)
    calls = OrderedDict()
    n = 0
    m = 0
    with SpliceAI() as caller:
        try:
            for record in vcf_reader:
                n += 1
                chromosome = record.CHROM
                if chromosome.startswith("chr"):
                    chromosome = chromosome[3:]
                pos = record.POS
                alt_list = [s.sequence for s in record.ALT]
                details, prediction, score = caller.get_all(chromosome, pos, record.REF, alt_list)
                if prediction == "None":
                    continue
                calls[(record.CHROM, pos)] = (details, prediction, score)
                if (len(calls) > 1000):
                    m += len(calls)
                    flush(calls, call_file)
                    print "{}: {}; {} / {}".format(chromosome, pos, n, m)
        except Exception, e:
            print "Error in {}: {}".format(record.CHROM, pos)
            print str(e)

        flush(calls, call_file)


def flush(calls, calls_file):
    with open(calls_file, "a") as f:
        for key in calls:
            call = calls[key]
            p = [str(k) for k in key]
            line = '\t'.join(p + [call[1], str(call[2])])
            f.write(line + '\n')
    calls.clear()

def apply_calls(calls_file, output_file, header_file, tags, input_vcf):
    execute("bgzip -f {}".format(calls_file))
    execute("tabix -s1 -b2 -e2 -f {}.gz".format(calls_file))
    columns = ','.join(["CHROM", "POS"] + list(tags))
    execute("bcftools annotate -a {}.gz -h {} -c {} -o {} {}".
        format(calls_file, header_file, columns, output_file, input_vcf))


def run(vcf_file):
    call_file = "splice_ai_calls.tsv"
    header_file = "splice_ai_calls.hdr"
    x = vcf_file.split('.')
    x = x[:-1] + ["spliceai"] + x[-1:]
    out_file = '.'.join(x)
    tags = OrderedDict()
    tags["SPLICE_AI_PRED"]=["String","Splice AI prediction for pathogenicity"]
    tags["SPLICE_AI_SCORE"]=["Float","Splice AI maximum score"]
    with open(header_file, "w") as h:
        pattern = '##INFO=<ID={tag},Number={n},Type={type},Description="{desc}">\n'
        for tag in tags:
            h.write(pattern.format(tag=tag, n=1, type=tags[tag][0], desc=tags[tag][1]))
    with open(call_file, "w") as f:
        f.write("# CHROM\tPOS\t{}\n".format('\t'.join(tags)))
    call(vcf_file, call_file)
    apply_calls(call_file, out_file, header_file, tags, vcf_file)



if __name__ == '__main__':
    run(sys.argv[1])