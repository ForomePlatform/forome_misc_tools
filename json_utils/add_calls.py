import argparse
import gzip
import json


def read_calls(calls_file, header_file):
    metadata = dict()
    with open(header_file) as hdr:
        for line in hdr:
            if not line.startswith("##INFO="):
                continue
            info = line.strip()[len("##INFO=<"):-1]
            id = None
            number = None
            for x in info.split(','):
                xx = x.split('=')
                if xx[0] == "ID":
                    id = xx[1]
                elif xx[0] == "Number":
                    number = int(xx[1])
            metadata[id] = number
    calls = dict()
    header = []
    with open(calls_file) as content:
        for line in content:
            if not header:
                if line.startswith('#'):
                    header = [t.strip() for t in line[1:].split()]
                    assert header[0] == "CHROM"
                    assert header[1] == "POS"
                    continue
                else:
                    raise Exception("Missing header")
            if line.startswith('#'):
                continue
            if not line.strip():
                continue
            tokens = [t.strip() for t in line.split()]
            key = {header[i]:tokens[i] for i in range(0, 2)}
            data = {
                header[i]:tokens[i] for i in range(2, len(header))
            }
            for k in data:
                if metadata[k] < 1:
                    data[k] = ""
            calls[(key["CHROM"], int(key["POS"]))] = data
    print "Read {} calls".format(len(calls))
    return calls, metadata


def process_json(a_json, u_json, calls, replace, metadata):
    n = 0
    n1 = 0
    n2 = 0
    for line in a_json:
        n += 1
        data = json.loads(line)
        if (n % 10000 == 0):
            print "{}/{}/{}".format(n, n1, n2)
        if (data["record_type"] != "variant"):
            u_json.write(line)
            continue
        edited = False
        if replace:
            for annotation in metadata:
                c = data["view"]["bioinformatics"]["called_by"]
                if annotation in c:
                    c.remove(annotation)
                    edited = True
                    n2 += 1
        chrom = data["data"]["seq_region_name"]
        if not chrom.startswith("chr"):
            chrom = "chr" + chrom
        pos = int(data["data"]["start"])
        if not edited and (chrom, pos) not in calls:
            u_json.write(line)
            continue
        if edited and (chrom, pos) not in calls:
            updated_line = json.dumps(data) + '\n'
            u_json.write(updated_line)
            continue
        call = calls[(chrom, pos)]
        for caller in call:
            value = call[caller]
            data["view"]["bioinformatics"]["called_by"].append(caller)
            if value:
                data["view"]["bioinformatics"]["caller_data"][caller] = value
            n1 += 1
        updated_line = json.dumps(data) + '\n'
        u_json.write(updated_line)


def update_json(json_file, calls, replace, metadata):
    input_path = json_file.split('.')
    is_gzip = input_path[-1] == 'gz'
    if is_gzip:
        idx = -2
    else:
        idx = -1
    output_path = input_path[:idx] + ['updated'] + input_path[idx:]
    output_file = '.'.join(output_path)

    if is_gzip:
        with gzip.open(json_file) as a_json, gzip.open(output_file, "wb") as u_json:
            process_json(a_json, u_json, calls, replace, metadata)
    else:
        with open(json_file) as a_json, open(output_file, "w") as u_json:
            process_json(a_json, u_json, calls, replace, metadata)

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Add calls annotation to anfisa json file")
    parser.add_argument("-i", "--input", dest = "input", help="Input JSON file", required=True)
    parser.add_argument("-c", "--calls", help="File with calls in tsv format", required=True)
    parser.add_argument("-d", "--header", help="VCF Header file", default="header.vcf_utils", required=True)
    parser.add_argument("-r", "--replace", action="store_true",
            help="Replace existing calls", required=False)

    args = parser.parse_args()
    print args

    calls, metadata = read_calls(args.calls, args.header)
    update_json(args.input, calls, args.replace, metadata)

