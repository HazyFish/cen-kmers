# Cen K-mers

## Reproduction

### Host Environment

- Create an Azure VM with Temporary Disk
- [Format and Initialize Temp NVMe Disk](https://learn.microsoft.com/en-us/azure/virtual-machines/enable-nvme-temp-faqs#how-can-i-format-and-initialize-temp-nvme-disks-in-linux-)

### Repo Setup

- `sudo apt install sra-toolkit jellyfish`
- [Install `uv`](https://docs.astral.sh/uv/getting-started/installation/)
- Clone this repository
- `snakemake --cores all --resources mem_mb=163840 --keep-going`

- `snakemake --snakefile Snakefile.hamburg --cores all --resources mem_mb=163840 --keep-going --dry-run`
- `python scripts/join_kmer_counts_per_genotype.py PRJNA723952.SraRunTable.csv data/hamburg/normalized-filtered-counts/ -o data/hamburg/joined-counts --feather-suffix .f32.feather -j 40`
