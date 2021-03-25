import json

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import seaborn as sns
sns.set_theme(style='whitegrid')


def plot_performance_embl_fs(path, chunk_key='25_125_125'):
    with open('./results/embl_groupshare.json') as f:
        res_gfs = json.load(f)
    with open('./results/embl_scratch.json') as f:
        res_scratch = json.load(f)
    with open('./results/embl_tmp.json') as f:
        res_tmp = json.load(f)

    results = {'g': res_gfs, 'scratch': res_scratch, 'tmp': res_tmp}

    data = []
    for format_ in ('h5', 'n5', 'zarr'):
        compression = 'raw'
        size = res_gfs[format_]['sizes']
        for name, res in results.items():
            read = np.min(res[format_]['read'][compression][chunk_key])
            write = np.min(res[format_]['write'][compression][chunk_key])
            thr_read = size / 1e3 / read
            thr_write = size / 1e3 / write
            data.append([format_, name, read, write, size, thr_read, thr_write])

    data = pd.DataFrame(data, columns=['format', 'filesystem', 't-read [s]', 't-write [s]', 'size [MB]',
                                       r'thrpt-read [GB/s]', r'thrpt-write [GB/s]'])

    fig, axes = plt.subplots(3)
    sns.barplot(data=data, ax=axes[0], x="format", y="t-read [s]", hue="compression")
    sns.barplot(data=data, ax=axes[1], x="format", y="t-write [s]", hue="compression")
    sns.barplot(data=data, ax=axes[2], x="format", y="size [MB]", hue="compression")
    plt.show()


if __name__ == '__main__':
    plot_performance_embl_fs()
