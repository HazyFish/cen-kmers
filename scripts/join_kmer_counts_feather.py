import subprocess
import sys
from math import ceil
from pathlib import Path
from typing import Annotated

import pandas as pd
import typer
from typer import Argument, Option


def join_kmer_counts(
    input_feather_paths: Annotated[list[Path], Argument()],
    output_feather_path: Annotated[Path, Option("-o")],
    max_jobs: Annotated[int, Option("-j")] = 1,
    min_batch_size: Annotated[int, Option("-b")] = 2,
    delete_inputs: Annotated[
        bool,
        Option("--delete-inputs/--keep-inputs"),
    ] = False,
) -> None:
    while max_jobs > 1 and len(input_feather_paths) > min_batch_size * 1.5:
        input_size = len(input_feather_paths)
        batch_size = max(min_batch_size, ceil(input_size / max_jobs))

        subprocesses = [
            subprocess.Popen(
                args=[
                    sys.executable,
                    __file__,
                    "-o",
                    str(
                        output_feather_path.with_suffix(
                            f".{input_size}.{i // batch_size}.feather.tmp"
                        )
                    ),
                    "-j",
                    "1",
                    "-b",
                    str(min_batch_size),
                    "--delete-inputs" if delete_inputs else "--keep-inputs",
                    *(str(path) for path in input_feather_paths[i : i + batch_size]),
                ],
            )
            for i in range(0, input_size, batch_size)
        ]

        print(
            f"Joining {input_size} tables into {len(subprocesses)} with batch size {batch_size}..."
        )

        for proc in subprocesses:
            assert proc.wait() == 0

        input_feather_paths = [
            output_feather_path.with_suffix(f".{input_size}.{i}.feather.tmp")
            for i in range(len(subprocesses))
        ]
        delete_inputs = True

    df = pd.read_feather(input_feather_paths[0])

    for input_feather_path in input_feather_paths[1:]:
        df = df.join(
            other=pd.read_feather(input_feather_path),
            how="outer",
        )

    df.to_feather(output_feather_path)

    if delete_inputs:
        for input_feather_path in input_feather_paths:
            input_feather_path.unlink(missing_ok=True)


if __name__ == "__main__":
    typer.run(join_kmer_counts)
