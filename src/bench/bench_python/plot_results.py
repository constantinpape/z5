"""Plot the results produced by ``bench_zarr_v3.py``.

Reads the JSON results file and produces, into ``--out-dir``:

* one **throughput** figure per ``(operation, thread-mode)`` -- grouped bars of MB/s
  (x = codec, hue = library), with one subplot per ``dim x {sharded, unsharded}`` panel;
* one **compression-ratio** figure -- bars per ``(dim, sharded)`` x codec x library.
  The ratio is library-independent for a given codec/level, so this doubles as a sanity
  check that the three libraries were configured equivalently.

Example::

    python plot_results.py --results bench_v3_results.json --out-dir ./plots
"""
import argparse
import json
import os

import matplotlib
matplotlib.use("Agg")  # headless: write PNGs without a display
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

try:
    import seaborn as sns
    sns.set_theme(style="whitegrid")
    HAVE_SNS = True
except Exception:
    HAVE_SNS = False

LIB_ORDER = ["z5py", "zarr", "tensorstore"]
CODEC_ORDER = ["raw", "gzip", "blosc", "zstd"]


def load(results_path):
    with open(results_path) as f:
        blob = json.load(f)
    df = pd.DataFrame(blob["records"])
    df["panel"] = df["dim"] + "/" + np.where(df["sharded"], "sharded", "unsharded")
    return blob["meta"], df


def _ordered(values, order):
    present = [v for v in order if v in set(values)]
    present += [v for v in sorted(set(values)) if v not in order]
    return present


def _grouped_bars(ax, sub, value, title):
    """Grouped bar chart: x = codec, one bar group per library."""
    codecs = _ordered(sub["codec"], CODEC_ORDER)
    libs = _ordered(sub["library"], LIB_ORDER)
    x = np.arange(len(codecs))
    width = 0.8 / max(1, len(libs))
    for i, lib in enumerate(libs):
        vals = [sub[(sub["codec"] == c) & (sub["library"] == lib)][value].max()
                for c in codecs]
        ax.bar(x + i * width, vals, width, label=lib)
    ax.set_xticks(x + width * (len(libs) - 1) / 2)
    ax.set_xticklabels(codecs)
    ax.set_title(title)
    ax.legend(fontsize=8)


def plot_throughput(meta, df, out_dir):
    panels = _ordered(df["panel"], [d + "/" + s for d in ("2d", "3d")
                                    for s in ("unsharded", "sharded")])
    for op in sorted(df["op"].unique()):
        for nt in sorted(df["threads"].unique()):
            sel = df[(df["op"] == op) & (df["threads"] == nt)]
            if sel.empty:
                continue
            ncol = len(panels)
            fig, axes = plt.subplots(1, ncol, figsize=(4.2 * ncol, 4.2), squeeze=False)
            for ax, panel in zip(axes[0], panels):
                _grouped_bars(ax, sel[sel["panel"] == panel],
                              "throughput_mbps", panel)
                ax.set_ylabel("MB/s")
            mode = "single-threaded" if nt == 1 else "%d threads" % nt
            fig.suptitle("%s throughput -- %s" % (op, mode), fontweight="bold")
            fig.tight_layout()
            fname = os.path.join(out_dir, "throughput_%s_t%d.png" % (op, nt))
            fig.savefig(fname, dpi=120)
            plt.close(fig)
            print("wrote", fname)


def plot_ratio(meta, df, out_dir):
    # ratio is op- and thread-independent; collapse to one row per (panel, codec, library)
    sub = (df.groupby(["panel", "codec", "library"], as_index=False)["ratio"].max())
    panels = _ordered(sub["panel"], [d + "/" + s for d in ("2d", "3d")
                                     for s in ("unsharded", "sharded")])
    ncol = len(panels)
    fig, axes = plt.subplots(1, ncol, figsize=(4.2 * ncol, 4.2), squeeze=False)
    for ax, panel in zip(axes[0], panels):
        _grouped_bars(ax, sub[sub["panel"] == panel], "ratio", panel)
        ax.set_ylabel("compression ratio (raw / stored)")
        ax.axhline(1.0, color="k", lw=0.8, ls="--")
    fig.suptitle("compression ratio by codec", fontweight="bold")
    fig.tight_layout()
    fname = os.path.join(out_dir, "compression_ratio.png")
    fig.savefig(fname, dpi=120)
    plt.close(fig)
    print("wrote", fname)


def main(argv=None):
    p = argparse.ArgumentParser(description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--results", "-r", default="bench_v3_results.json",
                   help="results JSON produced by bench_zarr_v3.py")
    p.add_argument("--out-dir", "-o", default="./plots", help="output directory for PNGs")
    args = p.parse_args(argv)

    os.makedirs(args.out_dir, exist_ok=True)
    meta, df = load(args.results)
    if not HAVE_SNS:
        print("(seaborn not installed -- using plain matplotlib styling)")
    plot_throughput(meta, df, args.out_dir)
    plot_ratio(meta, df, args.out_dir)


if __name__ == "__main__":
    main()
