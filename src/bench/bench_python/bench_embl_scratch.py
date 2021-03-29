import os
import json

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import seaborn as sns

from bench import main
sns.set_theme(style='whitegrid')


def run_benchmark():
    scratch_root = '/scratch/pape/io-bench/stripes'
    scratch_folders = os.listdir(scratch_root)
    print(scratch_folders)

    compressors = ['raw']
    data_path = '/g/kreshuk/pape/Work/data/cremi/original/sample_A_20160501.hdf'
    for scratch_folder in scratch_folders:
        save_folder = os.path.join(scratch_root, scratch_folder, 'data')
        main(data_path, scratch_folder, save_folder, compressors=compressors)


def evaluate_benchmark():
    root = './results/embl_scratch'
    names = os.listdir(root)

    formats = ['h5', 'n5', 'zarr']
    data = []

    compression = 'raw'
    chunk = '25_125_125'

    for name in names:
        with open(os.path.join(root, name)) as f:
            res = json.load(f)
            for format_ in formats:
                size = res[format_]['sizes'][compression][chunk]
                read = np.min(res[format_]['read'][compression][chunk])
                write = np.min(res[format_]['write'][compression][chunk])

                thr_read = size / 1e3 / read
                thr_write = size / 1e3 / write
                data.append([name, format_, thr_read, thr_write])

    cols = ['scratch', 'format', 'thrpt-read [GB/s]', 'thrpt-write [GB/s]']
    data = pd.DataFrame(data, columns=cols)

    fig, axes = plt.subplots(2)
    sns.barplot(data=data, ax=axes[0], x="scratch", y="thrpt-read [GB/s]", hue="format")
    sns.barplot(data=data, ax=axes[1], x="scratch", y="thrpt-write [GB/s]", hue="format")
    plt.savefig('embl-scratch-results.png')


if __name__ == '__main__':
    # run_benchmark()
    evaluate_benchmark()
