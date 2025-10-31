# helpers.py
from pathlib import Path
import urllib.request

_PUBLIC_GCS_HTTP = "https://storage.googleapis.com/"

def resolve_for_local(path: str, cache_dir: Path = Path("./.cache")) -> str:
    """
    If path is gs://bucket/obj and we’re running locally, fetch once via HTTPS
    (public GCS objects are readable without auth) and return a local filepath.
    Otherwise return the original path.
    """
    if path.startswith("gs://"):
        bucket_obj = path[5:]  # strip 'gs://'
        url = f"{_PUBLIC_GCS_HTTP}{bucket_obj}"
        cache_dir.mkdir(parents=True, exist_ok=True)
        local = cache_dir / Path(bucket_obj).name
        if not local.exists():
            urllib.request.urlretrieve(url, local)
        return str(local)
    return path
