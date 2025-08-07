from pathlib import Path
from typing import Annotated, Callable, Generator, List, Optional

import pandas as pd
import typer


def split_feather(
    input_feather: Path,
    n_parts: int,
    output_path_fn: Optional[Callable[[Path, int], Path]] = None,
) -> Generator[Path, None, None]:
    """
    Split a feather file into n_parts by hashing the index.
    Returns a list of output paths.
    """

    df = pd.read_feather(input_feather)

    if df.index.nlevels > 1:
        raise ValueError("Multi-level index is not supported.")

    if output_path_fn is None:
        output_path_fn = lambda input_path, i: input_path.with_suffix(
            f".part.{i+1}{input_path.suffix}"
        )

    for i in range(n_parts):
        part = df[(df.index.map(hash) % n_parts) == i]
        out_path = output_path_fn(input_feather, i)
        part.to_feather(out_path)
        yield out_path


def main(
    input_feather: Annotated[
        Path,
        typer.Argument(help="Input feather file"),
    ],
    n_parts: Annotated[
        int,
        typer.Option("--n-parts", "-n", help="Number of parts to split into"),
    ],
):
    print(f"Splitting {input_feather} into {n_parts} parts...")

    for out_path in split_feather(input_feather, n_parts):
        print(f"Wrote {out_path}")


if __name__ == "__main__":
    typer.run(main)
