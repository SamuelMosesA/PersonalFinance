from abnamroparser import tsvparser
from .base_statement_processor import BaseStatementProcessor
from psycopg2 import sql
import polars as pl
import json
from transaction_services.config.db_constants import TX_SCHEMA, DEBIT_TX_TABLE


class AbnStatementProcessor(BaseStatementProcessor):
    file_to_table_columnn_map = {
        "account": "account",
        "amount": "tx_amount",
        "currency": "currency",
        "date": "tx_date",
        "desc": "desc_json",
        "description": "description",
        "end_saldo": "end_balance",
        "start_saldo": "start_balance",
    }

    @staticmethod
    def _map_dtypes(df: pl.DataFrame) -> pl.DataFrame:
        return df.select(
            pl.col("tx_date").str.strptime(pl.Date),
            pl.col("tx_amount").cast(pl.Decimal),
            pl.col("start_balance").cast(pl.Decimal),
            pl.col("end_balance").cast(pl.Decimal),
            pl.col("account").cast(pl.String),
            pl.col("currency").cast(pl.String),
            pl.col("description").cast(pl.String),
            pl.col("desc_json").map_elements(json.dumps, return_dtype=pl.String),
        )

    def parse_file(file_path):
        data_as_json = tsvparser.convert_tsv_to_json_like(file_path)
        df = pl.DataFrame(data_as_json).rename(
            mapping=AbnStatementProcessor.file_to_table_columnn_map
        )
        df = AbnStatementProcessor._map_dtypes(df)
        return df

    def get_update_database_query(file_path, file_content: pl.DataFrame):
        columns = file_content.columns
        unique_constraint_columns = [c for c in columns if c not in ["desc_json"]]
        query = sql.SQL(
            "INSERT INTO {schema}.{table} ({columns}) VALUES ("
            + ",".join(["%s" for i in range(len(columns))])
            + ") ON CONFLICT ({unique_constraint_columns}) DO NOTHING"
        ).format(
            schema=sql.Identifier(TX_SCHEMA),
            table=sql.Identifier(DEBIT_TX_TABLE),
            columns=sql.SQL(", ").join(map(sql.Identifier, columns)),
            unique_constraint_columns=sql.SQL(", ").join(
                map(sql.Identifier, unique_constraint_columns)
            ),
        )
        return query
