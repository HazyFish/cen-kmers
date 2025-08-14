from pathlib import Path
from typing import Annotated, List

import pandas as pd
import typer
from typer import Argument, Option


def join_parts(
    input_files: Annotated[List[Path], Argument(help="List of feather files to join")],
    output_file: Annotated[Path, Option("-o", help="Output feather file")],
):
    if not input_files:
        raise ValueError("No input files provided.")

    dfs = []
    columns = None
    all_index = None

    for f in input_files:
        df = pd.read_feather(f)
        print(f"{f}: shape={df.shape}")

        if columns is None:
            columns = df.columns
        else:
            if not df.columns.equals(columns):
                raise ValueError(
                    f"File {f} has different columns: {df.columns} vs {columns}"
                )

        if all_index is None:
            all_index = df.index
        else:
            overlap = all_index.intersection(df.index)
            if not overlap.empty:
                raise ValueError(f"Index overlap detected in file {f}")
            all_index = all_index.union(df.index)

        dfs.append(df)

    result = pd.concat(dfs, axis=0)
    result.to_feather(output_file, compression="zstd", compression_level=8)
    print(f"Joined {len(input_files)} files into {output_file} {result.shape}")


if __name__ == "__main__":
    typer.run(join_parts)
