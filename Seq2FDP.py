import re
import time
import os
import sys

fname_log = sys.argv[1]
fdp_dir = fname_log = sys.argv[2]
fname_template = fdp_dir + "/inputs_template.json"
with open(fname_log,'r') as fin:
    text = fin.read()


def find_all_nested(s, left, right):
    result = []
    l = len(s)
    rexp = r"[{}{}]|$".format(left, right)
    pos_dict = {left: [], right: []}
    loc = 0
    gloc = 0  # global loc
    while gloc < l-1:
        # Wrong structure
        if len(pos_dict[left]) < len(pos_dict[right]):
            print(pos_dict)
            raise ValueError("Wrong structure!")

        # Normal pass
        next_hit = re.search(rexp, s)  # |$ makes sure something is always returned
        loc = next_hit.span(0)[0]
        gloc += loc + 1
        if next_hit.group(0):
            pos_dict[next_hit.group(0)].append(gloc)
            s = s[loc+1:]
            if len(pos_dict[left]) == len(pos_dict[right]):
                result.append((pos_dict[left][0]-1,pos_dict[right][-1]))
                pos_dict = {left: [], right: []}
    return result


nests = find_all_nested(text, "{", "}")
t = json.loads(text[nests[-1][0]:nests[-1][1]])
bamlist = t["outputs"]['ConvertPairedFastQsToUnmappedBamWf.output_bams']

# Edit template JSON
with open(fname_template,'r') as fin:
    template_dict = json.load(fin)
sample_dict = template_dict["WholeGenomeGermlineSingleSample.sample_and_unmapped_bams"]
timestr = str(time.time())
for i, bam in enumerate(bamlist):
    basename = bam.rstrip(".bam").rstrip(".unmapped")
    sample_dict["unmapped_bam_suffix"] = bam[len(basename):]
    basename = basename.split('/')[-1]
    sample_dict["base_file_name"] = basename
    sample_dict["flowcell_unmapped_bams"] = [bam]
    sample_dict["final_gvcf_base_name"] = basename


    # Create JSON of inputs
    json_name = "_".join(["inputs", timestr, str(i)+".json"])
    with open(json_name, 'w') as fout:
        json.dump(template_dict, fout, indent=0)

    # Run Screen with cromwell
    command = ""
    print("screen -dm -S S{} bash -c '{}'".format(str(i), command))
