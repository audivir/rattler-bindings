# rattler-bindings

Python bindings for `rattler-build`, running it as a subprocess and parsing its JSON logs.

# Prerequisites

- Python 3.10 or newer
- A `conda`-based environment with `rattler-build` installed under `$CONDA_PREFIX/bin`

# Installation

```bash
pip install git+https://github.com/audivir/rattler-bindings
```

# Usage

```python
from rattler_bindings import rattler_build, optimized_rattler_build

logs, returncode = rattler_build(recipe="recipe.yaml", output_dir="output")
```

# License

MIT, see `LICENSE`.
