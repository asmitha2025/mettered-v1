"""Filesystem helpers shared by local pipeline jobs."""

import logging
import os
import shutil
import stat
import time
import uuid


logger = logging.getLogger(__name__)


def retry(action, attempts: int = 60, delay_seconds: float = 0.5) -> None:
    """Run a filesystem action with retries for transient Windows/OneDrive locks."""
    last_error = None
    for attempt in range(attempts):
        try:
            action()
            return
        except PermissionError as exc:
            last_error = exc
            if attempt == attempts - 1:
                break
            time.sleep(delay_seconds)
    raise last_error


def reset_permissions_and_retry(func, path, _exc_info):
    """Allow shutil.rmtree to remove read-only files on Windows/OneDrive."""
    try:
        os.chmod(path, stat.S_IWRITE)
        func(path)
    except PermissionError:
        time.sleep(0.2)
        os.chmod(path, stat.S_IWRITE)
        func(path)


def recreate_dir(path: str) -> None:
    """Delete a directory if present, then recreate it.

    On Windows with OneDrive-backed folders, Parquet part files can briefly stay
    locked after a previous read. If direct deletion is denied, move the stale
    output aside so the next write still gets a clean target path.
    """
    if os.path.exists(path):
        try:
            retry(lambda: shutil.rmtree(path, onerror=reset_permissions_and_retry))
        except PermissionError:
            stale_path = f"{path}.stale-{int(time.time())}-{uuid.uuid4().hex[:8]}"
            retry(lambda: os.rename(path, stale_path))
            logger.warning(
                "Could not delete locked output directory %s; moved it to %s",
                path,
                stale_path,
            )
            try:
                retry(lambda: shutil.rmtree(stale_path, onerror=reset_permissions_and_retry))
            except PermissionError:
                logger.warning(
                    "Stale output directory %s is still locked and can be removed later.",
                    stale_path,
                )
    os.makedirs(path, exist_ok=True)
