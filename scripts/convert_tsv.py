from enum import StrEnum
from pathlib import Path
from typing import Annotated

import pandas as pd
import typer
from typer import Argument, Option


class DtypeOption(StrEnum):
    float32 = "float32"
    float64 = "float64"
    int32 = "int32"
    int64 = "int64"


def convert_tsv(
    input_tsv_path: Annotated[Path, Argument()],
    output_path: Annotated[Path, Option("-o")],
    dtype: Annotated[
        DtypeOption,
        Option("--dtype", help="Data type for counts, e.g., f4 for float32"),
    ] = DtypeOption.float32,
) -> None:
    count_col_name = input_tsv_path.stem

    df = pd.read_csv(
        input_tsv_path,
        sep="\t",
        header=None,
        names=["kmer", count_col_name],
        index_col="kmer",
    )

    df[count_col_name] = df[count_col_name].astype(dtype.value)

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
