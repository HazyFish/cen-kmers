from pathlib import Path
from typing import Annotated, Callable, List, Optional

import pandas as pd
import typer
from typer import Argument, Option


def join_parts(
    input_feather_paths: List[Path],
    output_feather_path: Path,
    transform_part: Optional[Callable[[pd.DataFrame], pd.DataFrame]] = None,
):
    if not input_feather_paths:
        raise ValueError("No input files provided.")

    dfs = []
    columns = None
    all_index = None

    for f in input_feather_paths:
        df = pd.read_feather(f)
        print(f"{f}: {df.shape}")

        if transform_part:
            df = transform_part(df)
            print(f"{f} after transform: {df.shape}")

        if columns is None:
            columns = df.columns
        else:
            if not df.columns.equals(columns) and set(df.columns) != set(columns):
                raise ValueError(
                    f"File {f} has different columns: {set(df.columns)} vs {set(columns)}"
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
    result.to_feather(output_feather_path, compression="zstd", compression_level=8)
    print(
        f"Joined {len(input_feather_paths)} files into {output_feather_path} {result.shape}"
    )


def main(
    input_feather_paths: Annotated[
        List[Path],
        Argument(help="List of feather files to join"),
    ],
    output_feather_path: Annotated[
        Path,
        Option("-o", help="Output feather file"),
    ],
):
    join_parts(
        input_feather_paths=input_feather_paths,
        output_feather_path=output_feather_path,
    )


if __name__ == "__main__":
    typer.run(main)
