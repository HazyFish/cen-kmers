from pathlib import Path
from typing import Annotated

import pandas as pd
import typer
from typer import Argument, Option


def normalize_kmer_count_by_coverage(
    input_tsv_path: Annotated[Path, Argument()],
    output_csv_path: Annotated[Path, Option("-o")],
    coverage: Annotated[float, Option("-c")],
) -> None:
    df = pd.read_csv(
        input_tsv_path,
        sep="\t",
        header=None,
        names=["kmer", "count"],
    )

    # Normalize the counts by the coverage
    df["count"] = df["count"] / coverage

    df.to_csv(
        output_csv_path,
        header=False,
        index=False,
    )


if __name__ == "__main__":
    typer.run(normalize_kmer_count_by_coverage)
