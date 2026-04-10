"""Multi-encoding CSV loader with fallback attempts."""

import pandas as pd
from utils.logger import get_logger

log = get_logger("loader")

# Ordered list of (encoding, separator) attempts
_ATTEMPTS = [
    ("utf-8",     ","),
    ("utf-8-sig", ","),
    ("latin-1",   ","),
    ("cp1252",    ","),
    ("utf-8",     ";"),
    ("utf-8",     "\t"),
]


def load_csv(path: str) -> pd.DataFrame:
    """Load a CSV file trying multiple encodings and separators.

    Returns the first successful parse that yields >= 5 columns.
    Normalizes column names to lowercase with underscores.
    """
    last_error = None

    for encoding, sep in _ATTEMPTS:
        try:
            df = pd.read_csv(path, encoding=encoding, sep=sep, low_memory=False)
            if df.shape[1] >= 5:
                # Normalize column names
                df.columns = (
                    df.columns
                    .str.strip()
                    .str.lower()
                    .str.replace(" ", "_", regex=False)
                )
                log.info(
                    f"Loaded successfully — encoding={encoding}, sep={repr(sep)}, "
                    f"shape={df.shape}, columns={list(df.columns)}"
                )
                return df
        except Exception as e:
            last_error = e
            log.debug(f"Failed with encoding={encoding}, sep={repr(sep)}: {e}")
            continue

    raise RuntimeError(
        f"Failed to load CSV from '{path}' after all encoding/separator attempts. "
        f"Last error: {last_error}"
    )
