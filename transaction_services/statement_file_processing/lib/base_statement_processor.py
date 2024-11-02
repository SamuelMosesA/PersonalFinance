from abc import ABC, abstractmethod
from pathlib import Path
from psycopg2.sql import SQL
import polars as pl


class BaseStatementProcessor(ABC):
    @staticmethod
    @abstractmethod
    def parse_file(file_path: Path) -> pl.DataFrame: ...

    @staticmethod
    @abstractmethod
    def get_update_database_query(
        file_path: Path, file_content: pl.DataFrame
    ) -> SQL: ...
