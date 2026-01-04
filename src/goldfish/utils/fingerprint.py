"""Data fingerprinting for Goldfish signals."""

import hashlib
import logging
from pathlib import Path
from typing import Any

logger = logging.getLogger("goldfish.utils.fingerprint")


def calculate_fingerprint(path: Path) -> dict[str, Any]:
    """Calculate fingerprint/stats for a data file.

    Supports:
    - .npy: shape, dtype, mean, std (if numpy installed)
    - .csv: columns, row count (if pandas installed)
    - Fallback: file size, sha256 hash

    Returns:
        Dict with fingerprint metadata
    """
    if not path.exists():
        return {}

    stats: dict[str, Any] = {
        "size_bytes": path.stat().st_size,
    }

    # Basic file hash (sha256 of first 1MB to keep it fast)
    try:
        sha256 = hashlib.sha256()
        with open(path, "rb") as f:
            chunk = f.read(1024 * 1024)
            sha256.update(chunk)
        stats["sha256_prefix"] = sha256.hexdigest()
    except Exception as e:
        logger.debug("Failed to calculate hash for %s: %s", path, e)

    suffix = path.suffix.lower()
    file_size = stats["size_bytes"]

    if suffix == ".npy":
        try:
            import numpy as np

            # Use mmap_mode="r" to avoid loading file into memory
            data = np.load(path, mmap_mode="r")
            stats.update(
                {
                    "type": "tensor",
                    "shape": list(data.shape),
                    "dtype": str(data.dtype),
                }
            )
            # Add deep stats
            # Thresholds:
            # - < 100MB: Full stats (fast enough for small tensors)
            # - >= 100MB: Reservoir sampling (O(1) vs O(N) memory/time)
            if file_size < 100 * 1024 * 1024:
                stats["mean"] = float(np.mean(data))
                stats["std"] = float(np.std(data))
                stats["sampled"] = False
            else:
                # Reservoir sampling for large tensors
                # We sample 10,000 elements to get a stable estimate
                sample_size = 10000
                total_elements = data.size
                if total_elements > sample_size:
                    # Flatten and sample
                    indices = np.random.choice(total_elements, sample_size, replace=False)
                    sample = data.flat[indices]
                    stats["mean"] = float(np.mean(sample))
                    stats["std"] = float(np.std(sample))
                    stats["sampled"] = True
                    logger.debug("Using reservoir sampling for large .npy (%d bytes)", file_size)
                else:
                    stats["mean"] = float(np.mean(data))
                    stats["std"] = float(np.std(data))
                    stats["sampled"] = False
        except ImportError:
            logger.debug("Numpy not found; skipping .npy deep fingerprint")
        except Exception as e:
            logger.debug("Failed to fingerprint .npy %s: %s", path, e)

    elif suffix == ".csv":
        try:
            import pandas as pd

            # Read only header for columns (fast regardless of file size)
            # We read the first line manually to avoid pandas scanning for newlines in large files
            with open(path) as f:
                header_line = f.readline()

            if header_line:
                import io

                df_head = pd.read_csv(io.StringIO(header_line))
                stats.update(
                    {
                        "type": "tabular",
                        "columns": list(df_head.columns),
                    }
                )

            # Faster row count for CSVs, but only if under 1GB
            if file_size < 1024 * 1024 * 1024:
                with open(path, "rb") as f:
                    stats["row_count"] = sum(1 for _ in f) - 1
            else:
                stats["row_count_approx"] = True  # Landmark for UI
                logger.debug("Skipping row count for large .csv (%d bytes)", file_size)
        except ImportError:
            logger.debug("Pandas not found; skipping .csv deep fingerprint")
        except Exception as e:
            logger.debug("Failed to fingerprint .csv %s: %s", path, e)

    return stats
