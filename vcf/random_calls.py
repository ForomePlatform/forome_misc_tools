import json
import gzip
import os
import random

MIN_SAMPLE_COUNT = 10
MIN_GENE_COUNT = 20
THRESHOLD1 = 0.1
THRESHOLD2 = 0.02

def select(dataset, subset, panel = []):
    samples = None
    ds = 0
    dg = 0.5 / len(panel) if panel else 0
    genes = {g:0 for g in panel}
    rand = random.Random(1)
    n = 0
    nn = 0
    with gzip.open(dataset) as f, open(subset, "w") as target:
        while True:
            line = f.readline()
            if (not line):
                break
            data = json.loads(line)
            record_type = data["record_type"]
            if record_type == "metadata":
                samples = {s:0 for s in data["samples"]}
                target.write(line)
                ds = 0.5 / len(samples)
                continue
            if (record_type != "variant"):
                continue

            n += 1
            if (n%1000) == 0:
                print "{}/{}".format(n, nn)
            v_samples = data["_filters"].get("has_variant")
            if not v_samples:
                continue
            v_genes = data["view"]["general"].get("genes")
            if not v_genes:
                continue

            r = rand.random()
            if (r > THRESHOLD1):
                continue
            r *= 10

            x = THRESHOLD2
            for sample in v_samples:
                if sample in samples:
                    c = samples[sample]
                    if (c < MIN_SAMPLE_COUNT):
                        x += ds
            if genes:
                for gene in v_genes:
                    if (gene in genes):
                        c = genes[gene]
                        if c < MIN_GENE_COUNT:
                            x += dg

            if (r > x):
                continue
            target.write(line)
            nn += 1
            for sample in v_samples:
                if sample in samples:
                    samples[sample] += 1
            if genes:
                for gene in v_genes:
                    if (gene in genes):
                        genes[gene] += 1


def read_panel(path):
    genes = []
    with open(path) as panel:
        for line in panel:
            if (not line.startswith('#')):
                genes.append(line.strip())
    return genes


if __name__ == '__main__':
    source = "/Users/misha/projects/bgm/cases/pf0003_wes/pf0003_anfisa.json.gz"
    target = "/Users/misha/projects/bgm/cases/pf0003_wes/test_anfisa.json"
    panels = []
    for panel in ["ttp1", "ttp3", "coagulation_system", "rep_purpura_fulminans"]:
        panels += read_panel(os.path.join("/Users/misha/projects/forome/versions/master/anfisa/app/config/files", panel + ".lst"))

    select(source, target, panels)