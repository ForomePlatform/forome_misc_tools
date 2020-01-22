import os
import vcf as pyvcf

def add_gt(src, dest, sample):
    n = 0
    with open(src) as vcf, open(dest, "w") as vcf2:
        for line in vcf:
            if line.startswith('##'):
                vcf2.write(line)
                continue
            elif line.startswith('#'):
                line = line.strip()
                vcf2.write('##FORMAT=<ID=GT,Number=1,Type=String,Description="Genotype">\n')
                vcf2.write(line + '\tFORMAT\t' + sample + '\n')
                continue

            n += 1
            line = line.strip()
            vcf2.write(line + "\tGT\t0/1\n")

    return


def get_genes(src):
    genes = dict()
    vcf = pyvcf.Reader(filename=src)
    for record in vcf:
        info = record.INFO
        if 'GI' in info and 'EXON' in info:
            g_list = info['GI']
            for g in g_list:
                if g not in genes:
                    genes[g] = set()
                genes[g].add(info['SVTYPE'])

    return genes


def create_cnv_file(src, dest, samples):
    vcf = pyvcf.Reader(filename=src)
    cnv = []
    for record in vcf:
        info = record.INFO
        if 'TI' in info and 'EXON' in info:
            transcripts = info["TI"]
            svtype = info["SVTYPE"]
            if 'END' in info:
                end = info['END']
            else:
                length = -int(info["SVLEN"][0])
                end = record.POS + length
            for i in range(0, len(transcripts)):
                data = None
                if svtype == "DEL":
                    data = [record.CHROM,
                            str(record.POS),
                            str(end),
                            "1",
                            info["GI"][i],
                            transcripts[i],
                            "0/1",
                            "0",
                            "GERMLINE"
                            ]
                if data:
                    cnv.append(data)

    with open(dest, "w") as cnv_file:
        cnv_file.write("# Samples: {}\n".format(','.join(samples)))
        cnv_file.write(
            "#CHROM	START	END	EXON_NUM	GENE	TRANSCRIPT	GT	LO	TYPE")
        for data in sorted(cnv, cmp=cmp_c):
            cnv_file.write('\t'.join(data) + '\n')



def cmp_c(t1, t2):
    chr1 = t1[0]
    chr2 = t2[0]
    if chr1 == chr2:
        if int(t2[1]) == int(t1[1]):
            return int(t1[2]) - int(t2[2])
        return int(t1[1]) - int(t2[1])
    if (chr1.startswith('chr')):
        chr1 = chr1[3:]
    if (chr2.startswith('chr')):
        chr2 = chr2[3:]
    try:
        a1 = int(chr1)
    except:
        a1 = ord(chr1)
    try:
        a2 = int(chr2)
    except:
        a2 = ord(chr2)
    return a1 - a2



if __name__ == '__main__':
    dir = "/Users/misha/projects/bgm/cases/bgm0400_wgs"
    f1 = os.path.join(dir, "SVs.vcf")
    f2 = os.path.join(dir, "SV1.vcf")
    d = os.path.join(dir, "deletions.txt")
    #add_gt(f1, f2, "bgm0400a1")
    #get_genes(f1)
    create_cnv_file(f1, d, ["bgm0400a1"])
