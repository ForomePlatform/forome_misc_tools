import json

# CSQ = {"missense_variant", "synonymous_variant"}
CSQ = {
    "splice_acceptor_variant",
    "splice_donor_variant",
    "stop_gained",
    "frameshift_variant",
    "stop_lost",
    "start_lost",
    "inframe_insertion",
    "inframe_deletion",
    "missense_variant",
    "protein_altering_variant",
    "synonymous_variant",
    "splice_region_variant"
}

HEADER = [
    "id",
    "rsid",
    "gene",
    "gene_list",
    "n_PF",
    "n_Sep",
    "Q1",
    "Q2",
    "g_af",
    "g_af_popmax",
    "af_all",
    "af_pf",
    "af_sep",
    "csq",
    "splice_altering",
    "polyphen_hvar",
    "sift",
    "Status"
    "PF_samples",
    "Sepsis_samples"
]
COHORTS = ["PF", "SEPSIS"]

def transcript_data(data, type, gene):
    transcripts = [
        t for t in data["transcript_consequences"]
        if t.get("canonical") == 1 and t["source"] == "Ensembl"
           and t["gene_symbol"] == gene
    ]

    pset = set()
    for t in transcripts:
        if type in t:
            value = t[type]
            if isinstance(value, list):
                value = ','.join(value)
            pset.add(value)
    return ",".join(list(pset))


def predictions(filters, data, type, gene):
    plist = [p for p in filters[type] if p != "N/A"]
    if not plist:
        return None
    pset = set(plist)
    if len(pset) == 1:
        return plist[0]
    if type == "Polyphen_2_HVAR":
        type = "polyphen2_hvar_pred"
    elif type == "SIFT":
        type = "sift_pred"
    return transcript_data(data, type, gene)


def get_gene(data):
    transcripts = [
        t for t in data["transcript_consequences"]
        if t.get("canonical") == 1 and t["source"] == "Ensembl"
           and len(set(t['consequence_terms']) & CSQ) > 0
    ]
    gset = {t["gene_symbol"] for t in transcripts}
    #assert len(gset) == 1
    return list(gset)


def check_samples(quality, samples, f):
    Q = True
    for s in samples:
        qs = [q for q in quality if s in q["title"]]
        assert len(qs) == 1
        q = qs[0]
        Q = Q and f(q)
    return Q


def check_quality_2(quality, samples):
    qall = quality[0]
    Q2 = (qall["qd"] >= 4 and qall["fs"] <= 30)
    Q2 = Q2 and check_samples(quality, samples, lambda q: q["genotype_quality"] >= 20)
    return Q2


def check_ab(q, alt, threshold):
    a = q["allelic_depth"]
    ads = {s.split(':')[0]: s.split(':')[1] for s in a.split(',')}
    try:
        ad = int(ads.get(alt))
    except:
        return False
    rd = q['read_depth']
    ab = float(ad) / rd
    return ab > threshold


def check_quality_1(ft, quality, samples, alt):
    Q1 = check_samples(quality, samples, lambda q: q["read_depth"] >= 10)
    if not Q1:
        return False
    if "PASS" in ft:
        Q1 = check_samples(quality, samples, lambda q: q["genotype_quality"] >= 90)
    elif 'VQSRTrancheINDEL99.00to99.90' in ft:
        Q1 = check_samples(quality, samples, lambda q: check_ab(q, alt, 0.25))
    else:
        Q1 = False
    return Q1


def missing_genotype_rate(quality, patterns):
    for q in quality:
        s = q["title"]
        if not any([s.startswith(p) for p in patterns]):
            continue
        gt = q["genotype"]


def variant_for_gene(view, filters, data, gene, summary):
    quality = view["view"]["quality_samples"]
    label = data["label"]
    id = label.split(' ')[1]
    rsid = data["id"]
    gene_lists = [
        p for p in filters["Panels"]
            if p in ["Coagulation_System", "Purpura_Fulminans"]
    ]
    assert len(gene_lists) == 1
    gene_list = gene_lists[0]
    if gene_list == "Purpura_Fulminans":
        gene_list = "Complement_System"

    samples = [
        s for s in  filters["Has_Variant"]
            if s.startswith("bgm") or s.startswith("SRR")
    ]
    cohorts = dict()
    cohorts["PF"] = [s for s in samples if s.startswith("bgm")]
    cohorts["SEPSIS"] = [s for s in samples if s.startswith("SRR")]
    n_PF = len(cohorts["PF"])
    n_Sep = len(cohorts["SEPSIS"])

    if "alt" in data:
        alt = data["alt"]
    else:
        alt = view["view"]["general"]['alt']
    Q1 = check_quality_1(filters["FT"], quality, samples, alt)
    Q2 = check_quality_2(quality, samples)
    g_af = filters["gnomAD_AF"]
    g_af_popmax = filters["gnomAD_PopMax_AF"]
    af_all = filters["ALL_AF"]
    af_pf = filters["pf_AF"]
    af_sep = filters["sepsis_AF"]
    #csq = filters["Canonical_Annotation"]
    csq = transcript_data(data, "consequence_terms", gene)
    splice_altering = filters["splice_altering"]

    polyphen_hvar = predictions(filters, data, "Polyphen_2_HVAR", gene)
    sift = predictions(filters, data, "SIFT", gene)

    if "synonymous" in csq:
        status = "SYN"
    elif "missense" in csq:
        if polyphen_hvar:
            if 'P' in polyphen_hvar or 'D' in polyphen_hvar:
                status = "DAMAGING"
            else:
                status = "NEUTRAL"
        elif sift:
            if 'deleterious' in sift or 'tolerated_low_confidence' in sift:
                status = "DAMAGING"
            else:
                status = "NEUTRAL"
        else:
            raise Exception("No predictions!")
    else:
        status = "LOF"

    if Q1:
        for cohort in COHORTS:
            key = (gene_list, cohort, "DAMAGING" if status == "LOF" else status)
            if key in summary:
                sampl_set = summary[key]
            else:
                sampl_set = set()
                summary[key] = sampl_set
            sampl_set.update(cohorts[cohort])

    variant = [
        id,
        rsid,
        gene,
        gene_list,
        n_PF,
        n_Sep,
        Q1,
        Q2,
        g_af,
        g_af_popmax,
        af_all,
        af_pf,
        af_sep,
        csq,
        splice_altering,
        polyphen_hvar,
        sift,
        status,
        ','.join(cohorts["PF"]),
        ','.join(cohorts["SEPSIS"])
    ]

    return [str(v) for v in variant]


def read_variant(js_v, js_f, summary):
    view = json.loads(js_v)
    filters = json.loads(js_f)

    chrom = filters["Chromosome"]
    pos = filters["Start_Pos"]

    data = view["data"]

    assert ("chr" + data["seq_region_name"]) == chrom
    assert data["start"] == pos

    if (filters["Num_Genes"] == 1):
        gene = filters["Symbol"][0]
        return [variant_for_gene(view, filters, data, gene, summary)]

    return [variant_for_gene(view, filters, data, gene, summary) for gene in get_gene(data)]


def process():
    summary = dict()
    with open("fdata.json") as f_in, open("vdata.json") as v_in, open("data.csv", "w") as output:
        output.write(' \t'.join(HEADER) + '\n')
        while(True):
            js_v = v_in.readline().strip()
            if not js_v:
                break
            js_f = f_in.readline()
            variants = read_variant(js_v, js_f, summary)
            for v in variants:
                output.write(' \t'.join(v) + '\n')

    panels = {key[0] for key in summary.keys()}
    cohorts = {key[1] for key in summary.keys()}
    statuses = list({key[2] for key in summary.keys()})

    for panel in panels:
        with open(panel + "_summary.csv", "w") as output:
            output.write('\t'.join(["Cohort"] + statuses) + '\n')
            for cohort in cohorts:
                data = [
                    str(len(summary[(panel, cohort, status)]))
                        for status in statuses
                ]
                output.write('\t'.join([cohort] + data) + '\n')

    return


if __name__ == '__main__':
    process()