# environments

Conda environment files for a complete build environment.

## Usage

### Unix

Assumes `PY_VERSION` is set to desired python version, in `<major><minor>` format

```bash
conda env create -f environments/unix/$PY_VERSION.yml
conda activate z5-py$PY_VERSION
```

Set environment variables used by cmake

```bash
source environments/unix/env.sh
```

### Windows

to do
