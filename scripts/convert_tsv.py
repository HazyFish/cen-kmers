from pathlib import Path
from typing import Annotated

import numpy as np
import pandas as pd
import typer
from typer import Argument, Option


def convert_tsv(
    input_tsv_path: Annotated[Path, Argument()],
    output_path: Annotated[Path, Option("-o")],
) -> None:
    count_col_name = input_tsv_path.stem

    df = pd.read_csv(
        input_tsv_path,
        sep="\t",
        header=None,
        names=["kmer", count_col_name],
        index_col="kmer",
    )

    # Save space with using float32 instead of float64
    df[count_col_name] = df[count_col_name].astype(np.float32)

    if output_path.suffix == ".csv":
        df.to_csv(output_path)
    elif output_path.suffix == ".tsv":
        df.to_csv(output_path, sep="\t")
    elif output_path.suffix == ".feather":
        df.to_feather(output_path, compression="zstd", compression_level=8)
    elif output_path.suffix == ".parquet":
        df.to_parquet(output_path)
    else:
        raise ValueError(f"Unsupported output file format: {output_path.suffix}")


if __name__ == "__main__":
    typer.run(convert_tsv)
