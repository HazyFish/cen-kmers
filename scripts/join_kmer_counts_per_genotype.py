from pathlib import Path
from typing import Annotated

import pandas as pd
import typer
from join_kmer_counts_feather import join_kmer_counts


def main(
    csv_path: Annotated[
        Path,
        typer.Argument(help="CSV file with Run and genotype columns"),
    ],
    feather_dir: Annotated[
        Path,
        typer.Argument(
            help="Directory containing feather files (accession in filename)"
        ),
    ],
    output_dir: Annotated[
        Path,
        typer.Option(
            "--output-dir", "-o", help="Output directory for joined genotype tables"
        ),
    ],
    feather_suffix: Annotated[
        str,
        typer.Option(
            "--feather-suffix", help="Suffix for feather files (default: .feather)"
        ),
    ] = ".feather",
    max_jobs: Annotated[
        int,
        typer.Option("-j", help="Max jobs for join_kmer_counts"),
    ] = 1,
    min_batch_size: Annotated[
        int,
        typer.Option("-b", help="Min batch size for join_kmer_counts"),
    ] = 2,
):
    df = pd.read_csv(csv_path)
    if "Run" not in df.columns or "genotype" not in df.columns:
        raise ValueError("CSV must contain 'Run' and 'genotype' columns")

    output_dir.mkdir(parents=True, exist_ok=True)

    grouped = df.groupby("genotype")
    for genotype, group in grouped:
        accessions = group["Run"].tolist()

        feather_paths = [feather_dir / f"{acc}{feather_suffix}" for acc in accessions]
        feather_paths = [p for p in feather_paths if p.exists()]

        if not feather_paths:
            print(f"No feather files found for genotype {genotype}, skipping.")
            continue

        genotype_str = str(genotype)

        compact_genotype_str = (
            genotype_str.replace("Col-0", "Col")
            .replace("Ler-1", "Ler")
            .replace("asy1T142V-", "t")
            .replace("asy1", "a")
            .replace("/", "")
            .replace(" x ", "_")
        )

        output_path = output_dir / f"{compact_genotype_str}.joined.feather"

        if output_path.exists() or output_path.with_suffix(".index.feather").exists():
            print(f"Output for '{compact_genotype_str}' already exists, skipping.")
            continue

        print(
            f"Joining {len(feather_paths)} files for genotype '{genotype}' into {output_path}"
        )

        join_kmer_counts(
            input_feather_paths=feather_paths,
            output_feather_path=output_path,
            max_jobs=max_jobs,
            min_batch_size=min_batch_size,
            delete_inputs=False,
        )


if __name__ == "__main__":
    typer.run(main)
