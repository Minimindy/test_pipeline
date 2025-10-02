import argparse
from datetime import datetime
import pandas as pd
from evidently.report import Report
from evidently.metric_preset import DataDriftPreset
from pipelines.common import get_df, put_bytes

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--reference", required=True)
    ap.add_argument("--current", required=True)
    ap.add_argument("--artifacts-bucket", required=True)
    ap.add_argument("--out-prefix", default="monitor/textpipe")
    args = ap.parse_args()

    try:
        ref, cur = get_df(args.reference), get_df(args.current)
    except Exception as e:
        print(f"[monitor] skip: {e}")
        return

    rep = Report(metrics=[DataDriftPreset()])
    rep.run(
        reference_data=pd.DataFrame({"length": ref["text"].str.len(), "label": ref["label"]}),
        current_data=pd.DataFrame({"length": cur["text"].str.len(), "label": cur["label"]}),
    )
    html = rep.as_html()
    ts = datetime.utcnow().strftime("%Y%m%d-%H%M%S")
    put_bytes(args.artifacts_bucket, f"{args.out_prefix}/drift_{ts}.html", html.encode("utf-8"), "text/html")
    print("[monitor] drift report uploaded")

if __name__ == "__main__":
    main()
