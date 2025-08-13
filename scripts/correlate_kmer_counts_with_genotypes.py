from pathlib import Path
from typing import Callable, Optional, Union

import pandas as pd
import torch


def correlate_kmers_with_genotypes_torch(
    metadata_csv_path: Union[Path, str],
    genotype_csv_path: Union[Path, str],
    kmer_count_feather_path: Union[Path, str],
    kmer_filter: Optional[Callable[[pd.DataFrame], pd.DataFrame]] = None,
) -> pd.DataFrame:
    # Load metadata and genotype
    df_meta = pd.read_csv(metadata_csv_path, index_col="Run")
    df_g = pd.read_csv(genotype_csv_path, index_col=0)
    df_g = df_g[df_g.apply(lambda x: x.isin(["A", "H"]).all(), axis=1)]
    df_g.rename(index=lambda x: x.replace("-", "_"), inplace=True)
    df_g = df_g.map(lambda x: {"A": 0, "H": 1}.get(x, x))

    # Load kmer counts
    df = pd.read_feather(kmer_count_feather_path)
    df.rename(columns=lambda run: df_meta.loc[run, "Sample Name"], inplace=True)
    df.drop(columns=[col for col in df.columns if col not in df_g.index], inplace=True)
    df.fillna(0, inplace=True)
    df = df[df.sum(axis=1) > 0]

    df_g = df_g[df_g.index.isin(df.columns)]

    if kmer_filter is not None:
        df = kmer_filter(df)

    df_t = df.transpose()
    df_t = df_t.reindex(df_g.index)

    assert df_g.index.equals(df_t.index)

    X = torch.tensor(df_t.values, dtype=torch.float32)  # shape: [samples, kmers]
    corr_df = pd.DataFrame(index=df_t.columns)

    for chr in df_g.columns:
        y = torch.tensor(df_g[chr].values, dtype=torch.float32)  # shape: [samples]

        X_centered = X - X.mean(dim=0)
        y_centered = y - y.mean()

        numerator = (X_centered * y_centered.unsqueeze(1)).sum(dim=0)
        denominator = torch.sqrt((X_centered**2).sum(dim=0) * (y_centered**2).sum())
        corr = numerator / denominator
        corr_df[chr] = corr.cpu().numpy().astype("float32")

    return corr_df


def main():
    ecotype_map: dict[str, str] = {
        "aColaLer_Col": "AA_C",
        "Col_aColaLer": "C_AA",
        "Col_ColLer": "C_LC",
        "Col_tColaLer": "C_TA",
        "ColLer_Col": "LC_C",
        "tColaLer_Col": "TA_C",
    }

    joined_count_dir = Path("data/hamburg/joined-counts")
    cen_genotype_dir = Path("data/hamburg/cen-genotypes")

    corr_out_dir = Path("data/hamburg/correlations")
    corr_out_dir.mkdir(parents=True, exist_ok=True)

    for feather_path in joined_count_dir.glob("*.joined.part.*.feather"):
        genotype_csv_path = (
            cen_genotype_dir
            / f"{ecotype_map[feather_path.stem.split(".")[0]]}.cen-genotype.csv"
        )

        assert (
            genotype_csv_path.exists()
        ), f"Genotype file {genotype_csv_path} does not exist."

        print(f"Correlating {feather_path} with {genotype_csv_path}...")

        df_corr = correlate_kmers_with_genotypes_torch(
            metadata_csv_path="PRJNA723952.SraRunTable.csv",
            genotype_csv_path=genotype_csv_path,
            kmer_count_feather_path=feather_path,
        )

        out_path = corr_out_dir / f"{feather_path.stem}.correlation.feather"

        print(f"Dumping {out_path} {df_corr.shape}...")

        df_corr.to_feather(out_path)


if __name__ == "__main__":
    main()
