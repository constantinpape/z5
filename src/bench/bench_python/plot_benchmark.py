import argparse
import json

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import seaborn as sns
sns.set_theme(style='whitegrid')


def plot_benchmark(path, chunk_key='25_125_125', skip=[]):
    with open(path) as f:
        results = json.load(f)

    data = []
    for format_ in ('h5', 'n5', 'zarr'):
        read = results[format_]['read']
        write = results[format_]['write']
        sizes = results[format_]['sizes']
        compressions = list(read.keys())
        for compression in compressions:
            if compression in skip:
                continue
            t_read = np.min(read[compression][chunk_key])
            t_write = np.min(write[compression][chunk_key])
            size = sizes[compression][chunk_key]
            data.append([format_, compression, t_read, t_write, size])

    data = pd.DataFrame(data, columns=['format', 'compression', 't-read [s]', 't-write [s]', 'size [MB]'])

    fig, axes = plt.subplots(3)
    sns.barplot(data=data, ax=axes[0], x="format", y="t-read [s]", hue="compression")
    sns.barplot(data=data, ax=axes[1], x="format", y="t-write [s]", hue="compression")
    sns.barplot(data=data, ax=axes[2], x="format", y="size [MB]", hue="compression")
    plt.show()


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('path')
    parser.add_argument('--skip', nargs="+", default=[])
    args = parser.parse_args()
    plot_benchmark(args.path, skip=args.skip)
