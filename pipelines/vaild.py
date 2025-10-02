import argparse, sys
from pipelines.common import get_df

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", required=True)
    ap.add_argument("--min-rows", type=int, default=5)
    ap.add_argument("--min-classes", type=int, default=2)
    args = ap.parse_args()

    df = get_df(args.input)

    if len(df) < args.min_rows:
        sys.exit(f"[validate] too few rows: {len(df)} < {args.min_rows}")

    if df["label"].nunique() < args.min_classes:
        sys.exit(f"[validate] too few classes")

    if df["text"].isna().mean() > 0.05:
        sys.exit("[validate] too many nulls")

    print("[validate] OK")

if __name__ == "__main__":
    main()
