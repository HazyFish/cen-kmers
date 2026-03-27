from pathlib import Path
from typing import Annotated

import pandas as pd
import typer
from typer import Argument, Option


def compress_feathers(
    input_dir: Annotated[
        Path,
        Argument(help="Directory containing feather files to compress"),
    ],
    output_dir: Annotated[
        Path,
        Option("-o", help="Directory to write compressed feather files"),
    ],
    pattern: Annotated[
        str,
        Option("--pattern", help="Glob pattern for feather files"),
    ] = "*.feather",
    compression: Annotated[
        str,
        Option("--compression", help="Compression type (default: zstd)"),
    ] = "zstd",
    compression_level: Annotated[
        int,
        Option("--compression-level", help="Compression level (default: 8)"),
    ] = 8,
):
    output_dir.mkdir(parents=True, exist_ok=True)

    file_paths = list(input_dir.glob(pattern))

    if not file_paths:
        print(f"No files found in {input_dir} matching {pattern}")
        return

    print(f"Compressing {len(file_paths)} files from {input_dir} to {output_dir}...")

    for input_path in file_paths:
        output_path = output_dir / input_path.name
        df = pd.read_feather(input_path)
        df.to_feather(
            output_path,
            compression=compression,
            compression_level=compression_level,
        )
        print(f"Compressed {input_path} -> {output_path}")


if __name__ == "__main__":
    typer.run(compress_feathers)
