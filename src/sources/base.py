from abc import ABC, abstractmethod
from dataclasses import dataclass
from pathlib import Path


@dataclass
class DiscoveredFile:
    dataset: str
    period: str
    source_url: str
    source_format: str


@dataclass
class DownloadedFile:
    discovered: DiscoveredFile
    local_path: Path
    file_size_bytes: int


class SourceConnector(ABC):
    dataset_name: str

    @abstractmethod
    def discover(self, period: str) -> DiscoveredFile:
        raise NotImplementedError

    @abstractmethod
    def download(
        self,
        discovered: DiscoveredFile,
        destination_dir: Path,
        fixture_path: Path | None = None,
    ) -> DownloadedFile:
        raise NotImplementedError

    @abstractmethod
    def validate(self, downloaded: DownloadedFile) -> "ValidationResult":
        raise NotImplementedError


@dataclass
class ValidationResult:
    passed: bool
    row_count: int
    column_names: list[str]
    column_types: list[str]
    errors: list[str]
