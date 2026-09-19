from pathlib import Path

from src.common.hashing import schema_hash, sha256_bytes, sha256_file


def test_sha256_file_matches_known_digest(tmp_path: Path) -> None:
    file_path = tmp_path / "sample.txt"
    file_path.write_bytes(b"quanta-flow")

    digest = sha256_file(file_path)

    assert digest == sha256_bytes(b"quanta-flow")
    assert len(digest) == 64


def test_sha256_file_is_deterministic(tmp_path: Path) -> None:
    file_path = tmp_path / "sample.txt"
    file_path.write_bytes(b"repeat me")

    first = sha256_file(file_path)
    second = sha256_file(file_path)

    assert first == second


def test_schema_hash_changes_with_column_types() -> None:
    hash_a = schema_hash(["fare_amount"], ["Float64"])
    hash_b = schema_hash(["fare_amount"], ["Utf8"])

    assert hash_a != hash_b
