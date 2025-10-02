import argparse, io
import pandas as pd
import matplotlib.pyplot as plt
from datetime import datetime
from pipelines.common import get_df, put_df, put_bytes, parse_s3

def clean_text(s: str) -> str:
    return s.replace("\u3000", " ").strip() if isinstance(s, str) else ""

def fig_bytes(fig):
    buf = io.BytesIO()
    fig.savefig(buf, format="png", bbox_inches="tight")
    plt.close(fig)
    return buf.getvalue()

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", required=True)
    ap.add_argument("--output", required=True)
    ap.add_argument("--artifacts-bucket", required=True)
    ap.add_argument("--eda-prefix", default="reports/textpipe")
    args = ap.parse_args()

    df = get_df(args.input)
    df["text"] = df["text"].map(clean_text)
    df = df.dropna(subset=["text", "label"]).reset_index(drop=True)

    ts = datetime.utcnow().strftime("%Y%m%d-%H%M%S")

    # class distribution
    fig1 = plt.figure()
    df["label"].value_counts().plot(kind="bar")
    put_bytes(args.artifacts_bucket, f"{args.eda_prefix}/{ts}/class_dist.png", fig_bytes(fig1), "image/png")

    # text length histogram
    fig2 = plt.figure()
    df["len"] = df["text"].str.len()
    df["len"].hist(bins=20)
    put_bytes(args.artifacts_bucket, f"{args.eda_prefix}/{ts}/text_len.png", fig_bytes(fig2), "image/png")

    bucket, key = parse_s3(args.output)
    put_df(bucket, key, df.drop(columns=["len"], errors="ignore"))
    print(f"[process] cleaned -> {args.output}")

if __name__ == "__main__":
    main()
