"""Project path helpers.

Generated datasets default to the repository's data/ folder. Set
SUBSCRIPTION_PIPELINE_DATA_DIR to run the pipeline against another local data
root, which is useful for CI, Docker, Hugging Face, and locked OneDrive folders.
"""

import os


BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
DEFAULT_DATA_DIR = os.path.join(BASE_DIR, "data")


def data_dir() -> str:
    """Return the active data root directory."""
    return os.path.abspath(os.environ.get("SUBSCRIPTION_PIPELINE_DATA_DIR", DEFAULT_DATA_DIR))


def data_path(*parts: str) -> str:
    """Build an absolute path inside the active data root."""
    return os.path.join(data_dir(), *parts)


def display_data_path(*parts: str) -> str:
    """Return a concise path for logs."""
    if os.environ.get("SUBSCRIPTION_PIPELINE_DATA_DIR"):
        return data_path(*parts)
    return os.path.join("data", *parts)
