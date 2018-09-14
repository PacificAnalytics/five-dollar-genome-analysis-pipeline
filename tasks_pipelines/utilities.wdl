## Copyright Broad Institute, 2018
##
## This WDL defines utility tasks used for processing of sequencing data.
##
## Runtime parameters are often optimized for Broad's Google Cloud Platform implementation.
## For program versions, see docker containers.
##
## LICENSING :
## This script is released under the WDL source code license (BSD-3) (see LICENSE in
## https://github.com/broadinstitute/wdl). Note however that the programs it calls may
## be subject to different licenses. Users are responsible for checking that they are
## authorized to run all programs before running this script. Please see the docker
## page at https://hub.docker.com/r/broadinstitute/genomes-in-the-cloud/ for detailed
## licensing information pertaining to the included programs.

task CreateIntervalSubsetList {
  File exome_interval_list
  Int scatter_count = 50
  Int preemptible_tries

  # This script creates interval subset lists from a master list for scattering N-ways 
  # INPUT REQUIREMENTS
  # The script assumes that the master interval list is formatted according to Picard conventions 
  # LOGIC
  # The script attempts to partition the intervals in the master list into N subsets 
  # of consecutive intervals (set by "desired_N"), balanced so that the subsets all 
  # add up to roughly the same amount of genomic territory. Based on the desired N and
  # the "wiggle_factor" value, the script defines a maximum length of territory allowed 
  # per subset. It then iterates through all intervals, creating subsets and adding 
  # intervals until the max size is exceeded and a new subset is warranted. 
  # OUTPUT
  # The script outputs a set of Picard-style interval files as well as a JSON stub file that 
  # can be used as base list for a WDL's inputs JSON.
  command <<<

    set -e
    java -Xms1g -jar /usr/gitc/picard.jar \
      IntervalListTools \
      SUBDIVISION_MODE=BALANCING_WITHOUT_INTERVAL_SUBDIVISION_WITH_OVERFLOW \
      UNIQUE=true \
      SORT=true \
      INPUT=${exome_interval_list} \
      OUTPUT=${exome_interval_list}".sorted"

    python <<CODE
    import os
    import sys

    # CLI arguments
    master_list_file ="${exome_interval_list}"+".sorted"
    desired_N =${scatter_count}
    wiggle_factor = 4
    dir_name = "output_intervals"
    comment = "@CO\t" + "intervals scattered" + "${scatter_count}" + "-ways\n" #"@CO\t"+sys.argv[5]+"\n"

    # Read in the master list file contents: 
    with open(master_list_file, "r") as master_list:
	
        header_lines = []
        intervals_list = []
        longest_interval = 0

        for line in master_list:
               # store the header lines (starting with @) to serve as output stub
               if line.startswith("@"):
                        header_lines.append(line)
               else:
                        line_split = line.split("\t")
                        length = int(line_split[2])-int(line_split[1])
                        intervals_list.append((line, length))
			
                        # keep track of what is the longest interval
                        if length > longest_interval:
                                longest_interval = length

    print "Number of intervals: "+str(len(intervals_list))
    print "Longest interval was: "+str(longest_interval)

    # Determine what is the total territory covered by intervals
    total_length = 0
    for interval in intervals_list:
        total_length = total_length + interval[1]
	
    print "Total length of covered territory: "+str(total_length)

    # Determine what should be the theoretical maximum territory per subset 
    # based on the desired N
    max_length_per_subset = total_length / desired_N

    print "Theoretical max subset length: "+str(max_length_per_subset)

    # Distribute intervals to separate files

    interval_count = 0
    batch_count = 0
    current_batch = []
    current_length = 0
    length_so_far = 0
    batches_list = []

    print "Processing..."

    def dump_batch(msg):

        global batch_count
        global current_batch
        global current_length
        global length_so_far
        global interval_count
        global batches_list

        # increment appropriate counters
        batch_count +=1
        length_so_far = length_so_far + current_length
        # report batch stats
        print "\t"+str(batch_count)+". \tBatch of "+str(len(current_batch))+"\t| "+str(current_length)+" \t|"+msg+" \t| "+str(interval_count)+" \t| So far "+str(length_so_far)+" \t| Remains "+str(total_length-length_so_far)
        # store batch
        batches_list.append(current_batch)
        # reset everything
        current_batch = []
        current_length = 0
	
    for interval in intervals_list:

        interval_count +=1
        #print interval_count
	
        # Is this new interval above the length limit by itself?
        if interval[1] > max_length_per_subset:
                dump_batch("close-out")
                current_batch.append(interval)
                current_length = current_length + interval[1] 
                dump_batch("godzilla")
		
        # Is this new interval putting us above the length limit when added to the batch?
        elif current_length + interval[1] > max_length_per_subset+max_length_per_subset/wiggle_factor:
                dump_batch("normal")
                current_batch.append(interval)
                current_length = current_length + interval[1] 

        else:
                current_batch.append(interval)
                current_length = current_length + interval[1] 

    dump_batch("finalize")

    print "Done.\nGrouped intervals into "+str(len(batches_list))+" batches."
		
    # Write batches to files and compose a JSON stub
    counter = 0
    json_stub = ["{", "\t\"workflow.scattered_calling_intervals\": ["]
    os.mkdir(dir_name)
    for batch in batches_list:
        counter +=1
        path = dir_name
        with open(path+"/scattered_"+str(counter)+"_of_"+str(len(batches_list))+".interval_list", "w") as intervals_file:
                # Write out the header copied from the original
                for line in header_lines:
                    intervals_file.write("%s" % line)
                # Add a comment to the header
                intervals_file.write("%s" % comment)
                # Write out the intervals
                for interval in batch:
                    intervals_file.write("%s" % interval[0])	
	
        # add the json line				
        json_stub.append("\t\t\"gs://bucket/dir/"+path+"/scattered_n_of_N.interval_list\",")
    json_stub.append("\t]")
    json_stub.append("}")

    print "Wrote "+str(counter)+" interval files to \""+dir_name+"/scattered_n_of_N.interval_list\""			

    # Write out the json stub
    with open("scattered_intervals.json", "w") as json_file:
        for line in json_stub:
            json_file.write("%s\n" % line)

    print "Wrote a JSON stub to \"scattered_intervals.json\""
    CODE
  >>>
  runtime {
    preemptible: preemptible_tries
    docker: "us.gcr.io/broad-gotc-prod/genomes-in-the-cloud:2.3.2-1510681135"
    memory: "2 GB"
  }
  output {
    Array[File] scattered_interval_list = glob("output_intervals/scattered*")
    }
}

# Generate sets of intervals for scatter-gathering over chromosomes
task CreateSequenceGroupingTSV {
  File ref_dict
  Int preemptible_tries

  # Use python to create the Sequencing Groupings used for BQSR and PrintReads Scatter.
  # It outputs to stdout where it is parsed into a wdl Array[Array[String]]
  # e.g. [["1"], ["2"], ["3", "4"], ["5"], ["6", "7", "8"]]
  command <<<
    python <<CODE
    with open("${ref_dict}", "r") as ref_dict_file:
        sequence_tuple_list = []
        longest_sequence = 0
        for line in ref_dict_file:
            if line.startswith("@SQ"):
                line_split = line.split("\t")
                # (Sequence_Name, Sequence_Length)
                sequence_tuple_list.append((line_split[1].split("SN:")[1], int(line_split[2].split("LN:")[1])))
        longest_sequence = sorted(sequence_tuple_list, key=lambda x: x[1], reverse=True)[0][1]
    # We are adding this to the intervals because hg38 has contigs named with embedded colons and a bug in GATK strips off
    # the last element after a :, so we add this as a sacrificial element.
    hg38_protection_tag = ":1+"
    # initialize the tsv string with the first sequence
    tsv_string = sequence_tuple_list[0][0] + hg38_protection_tag
    temp_size = sequence_tuple_list[0][1]
    for sequence_tuple in sequence_tuple_list[1:]:
        if temp_size + sequence_tuple[1] <= longest_sequence:
            temp_size += sequence_tuple[1]
            tsv_string += "\t" + sequence_tuple[0] + hg38_protection_tag
        else:
            tsv_string += "\n" + sequence_tuple[0] + hg38_protection_tag
            temp_size = sequence_tuple[1]
    # add the unmapped sequences as a separate line to ensure that they are recalibrated as well
    with open("sequence_grouping.txt","w") as tsv_file:
      tsv_file.write(tsv_string)
      tsv_file.close()

    tsv_string += '\n' + "unmapped"

    with open("sequence_grouping_with_unmapped.txt","w") as tsv_file_with_unmapped:
      tsv_file_with_unmapped.write(tsv_string)
      tsv_file_with_unmapped.close()
    CODE
  >>>
  runtime {
    preemptible: preemptible_tries
    docker: "python:2.7"
    memory: "2 GB"
  }
  output {
    Array[Array[String]] sequence_grouping = read_tsv("sequence_grouping.txt")
    Array[Array[String]] sequence_grouping_with_unmapped = read_tsv("sequence_grouping_with_unmapped.txt")
  }
}

# This task calls picard's IntervalListTools to scatter the input interval list into scatter_count sub interval lists
# Note that the number of sub interval lists may not be exactly equal to scatter_count.  There may be slightly more or less.
# Thus we have the block of python to count the number of generated sub interval lists.
task ScatterIntervalList {
  File interval_list
  Int scatter_count
  Int break_bands_at_multiples_of

  command <<<
    set -e
    mkdir out
    java -Xms1g -jar /usr/gitc/picard.jar \
      IntervalListTools \
      SCATTER_COUNT=${scatter_count} \
      SUBDIVISION_MODE=BALANCING_WITHOUT_INTERVAL_SUBDIVISION_WITH_OVERFLOW \
      UNIQUE=true \
      SORT=true \
      BREAK_BANDS_AT_MULTIPLES_OF=${break_bands_at_multiples_of} \
      INPUT=${interval_list} \
      OUTPUT=out

    python3 <<CODE
    import glob, os
    # Works around a JES limitation where multiples files with the same name overwrite each other when globbed
    intervals = sorted(glob.glob("out/*/*.interval_list"))
    for i, interval in enumerate(intervals):
      (directory, filename) = os.path.split(interval)
      newName = os.path.join(directory, str(i + 1) + filename)
      os.rename(interval, newName)
    print(len(intervals))
    CODE
  >>>
  output {
    Array[File] out = glob("out/*/*.interval_list")
    Int interval_count = read_int(stdout())
  }
  runtime {
    docker: "us.gcr.io/broad-gotc-prod/genomes-in-the-cloud:2.3.2-1510681135"
    memory: "2 GB"
  }
}

# Convert BAM file to CRAM format
# Note that reading CRAMs directly with Picard is not yet supported
task ConvertToCram {
  File input_bam
  File ref_fasta
  File ref_fasta_index
  String output_basename
  Int preemptible_tries

  Float ref_size = size(ref_fasta, "GB") + size(ref_fasta_index, "GB")
  Int disk_size = ceil(2 * size(input_bam, "GB") + ref_size) + 20

  command <<<
    set -e
    set -o pipefail

    samtools view -C -T ${ref_fasta} ${input_bam} | \
    tee ${output_basename}.cram | \
    md5sum | awk '{print $1}' > ${output_basename}.cram.md5

    # Create REF_CACHE. Used when indexing a CRAM
    seq_cache_populate.pl -root ./ref/cache ${ref_fasta}
    export REF_PATH=:
    export REF_CACHE=./ref/cache/%2s/%2s/%s

    samtools index ${output_basename}.cram
  >>>
  runtime {
    docker: "us.gcr.io/broad-gotc-prod/genomes-in-the-cloud:2.3.2-1510681135"
    preemptible: preemptible_tries
    memory: "3 GB"
    cpu: "1"
    disks: "local-disk " + disk_size + " HDD"
  }
  output {
    File output_cram = "${output_basename}.cram"
    File output_cram_index = "${output_basename}.cram.crai"
    File output_cram_md5 = "${output_basename}.cram.md5"
  }
}

# Convert CRAM file to BAM format
task ConvertToBam {
  File input_cram
  File ref_fasta
  File ref_fasta_index
  String output_basename

  command <<<
    set -e
    set -o pipefail

    samtools view -b -o ${output_basename}.bam -T ${ref_fasta} ${input_cram}

    samtools index ${output_basename}.bam
  >>>
  runtime {
    docker: "us.gcr.io/broad-gotc-prod/genomes-in-the-cloud:2.3.2-1510681135"
    preemptible: 3
    memory: "3 GB"
    cpu: "1"
    disks: "local-disk 200 HDD"
  }
  output {
    File output_bam = "${output_basename}.bam"
    File output_bam_index = "${output_basename}.bam.bai"
  }
}

# Calculates sum of a list of floats
task SumFloats {
  Array[Float] sizes
  Int preemptible_tries

  command <<<
  python -c "print ${sep="+" sizes}"
  >>>
  output {
    Float total_size = read_float(stdout())
  }
  runtime {
    docker: "python:2.7"
    preemptible: preemptible_tries
  }
}
