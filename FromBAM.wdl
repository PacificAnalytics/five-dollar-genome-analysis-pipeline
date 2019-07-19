version 1.0

## Copyright Broad Institute, 2018
##
## This WDL pipeline implements data pre-processing and initial variant calling (GVCF
## generation) according to the GATK Best Practices (June 2016) for germline SNP and
## Indel discovery in human whole-genome data.
##
## Requirements/expectations :
## - Human whole-genome pair-end sequencing data in unmapped BAM (uBAM) format
## - One or more read groups, one per uBAM file, all belonging to a single sample (SM)
## - Input uBAM files must additionally comply with the following requirements:
## - - filenames all have the same suffix (we use ".unmapped.bam")
## - - files must pass validation by ValidateSamFile
## - - reads are provided in query-sorted order
## - - all reads must have an RG tag
## - GVCF output names must end in ".g.vcf.gz"
## - Reference genome must be Hg38 with ALT contigs
##
## Runtime parameters are optimized for Broad's Google Cloud Platform implementation.
## For program versions, see docker containers.
##
## LICENSING :
## This script is released under the WDL source code license (BSD-3) (see LICENSE in
## https://github.com/broadinstitute/wdl). Note however that the programs it calls may
## be subject to different licenses. Users are responsible for checking that they are
## authorized to run all programs before running this script. Please see the docker
## page at https://hub.docker.com/r/broadinstitute/genomes-in-the-cloud/ for detailed
## licensing information pertaining to the included programs.

# Local import
import "./tasks/UnmappedBamToAlignedBam.wdl" as ToBam
import "./tasks/AggregatedBamQC.wdl" as AggregatedQC
import "./tasks/GermlineVariantDiscovery.wdl" as Calling
import "./tasks/Qc.wdl" as QC
import "./tasks/Utilities.wdl" as Utils
import "./tasks/BamToCram.wdl" as ToCram
import "./tasks/BamToGvcf.wdl" as ToGvcf
import "./structs/GermlineStructs.wdl"

# WORKFLOW DEFINITION
workflow WholeGenomeGermlineSingleSample {
  input {
    GermlineSingleSampleReferences references
    PapiSettings papi_settings
    File wgs_coverage_interval_list
    File input_bam
    Float contamination
    File? haplotype_database_file
  }


  call ToGvcf.BamToGvcf as BamToGvcf {
    input:
      calling_interval_list = references.calling_interval_list,
      evaluation_interval_list = references.evaluation_interval_list,
      haplotype_scatter_count = references.haplotype_scatter_count,
      break_bands_at_multiples_of = references.break_bands_at_multiples_of,
      contamination = contamination,
      input_bam = input_bam,
      ref_fasta = references.reference_fasta.ref_fasta,
      ref_fasta_index = references.reference_fasta.ref_fasta_index,
      ref_dict = references.reference_fasta.ref_dict,
      dbsnp_vcf = references.dbsnp_vcf,
      dbsnp_vcf_index = references.dbsnp_vcf_index,
      base_file_name = "test1",
      final_gvcf_base_name = "test2",
      agg_preemptible_tries = papi_settings.agg_preemptible_tries
  }

  # Outputs that will be retained when execution is complete
  output {

    File gvcf_summary_metrics = BamToGvcf.gvcf_summary_metrics
    File gvcf_detail_metrics = BamToGvcf.gvcf_detail_metrics

    File output_vcf = BamToGvcf.output_vcf
    File output_vcf_index = BamToGvcf.output_vcf_index
  }
}
