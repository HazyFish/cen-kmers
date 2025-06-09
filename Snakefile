import pandas as pd

df = pd.read_csv('SraRunTable.csv', usecols=['Run', 'Bases'])
df['Coverage'] = df['Bases'] / 133_917_231  # Number of Bases in ColPEK Assembly
accessions = df['Run'].tolist()
coverages = df.set_index('Run')['Coverage'].to_dict()

rule all:
    input:
        expand(["tsvs/{acc}.tsv", "filtered-counts/{acc}.tsv"], acc=accessions)

rule download:
    output:
        "fqs/{acc}_1.fastq", "fqs/{acc}_2.fastq"
    params:
        tempdir="tmp/{acc}"
    threads: 6
    shell:'''
        mkdir -p {params.tempdir}
        fasterq-dump --threads {threads} --outdir fqs --temp {params.tempdir} --split-files --skip-technical {wildcards.acc}
        rm -rf {params.tempdir}
    '''

rule jellyfish_count:
    input:
        "fqs/{acc}_1.fastq", "fqs/{acc}_2.fastq"
    output:
        "jfs/{acc}.jf"
    resources:
        mem_mb=81920
    threads: 6
    shell:
        "jellyfish count -m 23 -s 16G -t {threads} -C -L 4 -o {output} {input}"

rule dump_tsv:
    input:
        "jfs/{acc}.jf"
    output:
        "tsvs/{acc}.tsv"
    shell:
        "jellyfish dump -c {input} > {output}"

rule dump_filtered_counts:
    input:
        "jfs/{acc}.jf"
    output:
        "filtered-counts/{acc}.tsv"
    params:
        min_count=lambda wildcards: int(coverages[wildcards.acc])
    shell:
        "jellyfish dump -c -t -L {params.min_count} {input} > {output}"
