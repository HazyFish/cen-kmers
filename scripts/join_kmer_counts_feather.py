import subprocess
import sys
from pathlib import Path
from typing import Annotated

import pandas as pd
import typer
from lib.average import average_of
from lib.balanced_partitioning import partition_with_balance
from split_feather import split_feather
from typer import Argument, Option


def average_file_size_of(files: list[Path]) -> float:
    return average_of([file.stat().st_size for file in files])


def join_kmer_counts(
    input_feather_paths: Annotated[list[Path], Argument()],
    output_feather_path: Annotated[Path, Option("-o")],
    max_jobs: Annotated[int, Option("-j")] = 1,
    min_batch_size: Annotated[int, Option("-b")] = 2,
    partition_threshold: Annotated[int, Option("--partition-threshold")] = 1024
    * 1024
    * 1024,
    delete_inputs: Annotated[
        bool,
        Option("--delete-inputs/--keep-inputs"),
    ] = False,
) -> None:
    while (
        max_jobs > 1
        and len(input_feather_paths) > min_batch_size * 1.5
        and average_file_size_of(input_feather_paths) < partition_threshold
    ):
        input_size = len(input_feather_paths)

        batches = partition_with_balance(
            input_feather_paths,
            n_groups=min(max_jobs, input_size // min_batch_size),
            weight_of=lambda x: x.stat().st_size,
        )

        subprocesses = [
            subprocess.Popen(
                args=[
                    sys.executable,
                    __file__,
                    "-o",
                    str(
                        output_feather_path.with_suffix(
                            f".{len(batches)}.{i}.feather.tmp"
                        )
                    ),
                    "-j",
                    "1",
                    "-b",
                    str(min_batch_size),
                    "--delete-inputs" if delete_inputs else "--keep-inputs",
                    *(str(path) for path in batch.items),
                ],
            )
            for i, batch in enumerate(batches)
        ]

        print(f"Joining {input_size} data frames into {len(batches)}...")

        for proc in subprocesses:
            assert proc.wait() == 0

        input_feather_paths = [
            output_feather_path.with_suffix(f".{len(batches)}.{i}.feather.tmp")
            for i in range(len(subprocesses))
        ]
        delete_inputs = True

    if average_file_size_of(input_feather_paths) < partition_threshold:
        print(
            f"Joining {len(input_feather_paths)} data frames into {output_feather_path}..."
        )

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
    else:
        n_parts = len(input_feather_paths)

        parts_map: dict[Path, list[Path]] = {}

        print("Splitting files into parts...")

        for input_feather_path in input_feather_paths:
            parts_map[input_feather_path] = list(
                split_feather(input_feather_path, n_parts)
            )

            if delete_inputs:
                input_feather_path.unlink(missing_ok=True)

        for i in range(n_parts):
            print(f"Joining part {i + 1}/{n_parts}...")

            join_kmer_counts(
                input_feather_paths=[parts[i] for parts in parts_map.values()],
                output_feather_path=output_feather_path.with_suffix(
                    f".part.{i+1}{output_feather_path.suffix}"
                ),
                max_jobs=max_jobs,
                min_batch_size=min_batch_size,
                delete_inputs=delete_inputs,
            )

        print(f"Creating index file...")

        def with_constant_column(
            df: pd.DataFrame,
            column_name: str,
            value,
        ) -> pd.DataFrame:
            df[column_name] = value
            df[column_name] = df[column_name].astype("int8")
            return df

        pd.concat(
            (
                with_constant_column(
                    pd.read_feather(
                        output_feather_path.with_suffix(
                            f".part.{i+1}{output_feather_path.suffix}"
                        ),
                        columns=["kmer"],
                    ),
                    "part",
                    i + 1,
                )
                for i in range(n_parts)
            ),
        ).to_feather(
            output_feather_path.with_suffix(f".index{output_feather_path.suffix}")
        )


if __name__ == "__main__":
    typer.run(join_kmer_counts)
