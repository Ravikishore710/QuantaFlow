import hashlib
from pathlib import Path


def sha256_file(path: Path, chunk_size: int = 1024 * 1024) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(chunk_size), b""):
            digest.update(chunk)
    return digest.hexdigest()


def sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def schema_hash(column_names: list[str], column_types: list[str]) -> str:
    fingerprint = "|".join(
        f"{name}:{dtype}" for name, dtype in zip(column_names, column_types, strict=False)
    )
    return sha256_bytes(fingerprint.encode("utf-8"))
