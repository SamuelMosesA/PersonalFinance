from abnamroparser import icspdfparser
from .base_statement_processor import BaseStatementProcessor
from psycopg2 import sql
import polars as pl
from pathlib import Path
from transaction_services.config.db_constants import TX_SCHEMA, CREDIT_CRD_TX_TABLE


class IcsCreditStatementProcessor(BaseStatementProcessor):
    file_to_table_columnn_map = {
        "card_number": "card_number",
        "amount": "tx_amount",
        "country_code": "country_code",
        "date": "tx_date",
        "descriptions": "descriptions",
        "exchange_rate": "exchange_rate",
        "foreign_amount": "foreign_amount",
        "foreign_currency": "foreign_currency",
    }

    @staticmethod
    def _map_dtypes(df: pl.DataFrame) -> pl.DataFrame:
        return df.select(
            pl.col("statement_id_in_file").cast(pl.Int64),
            pl.col("card_number").cast(pl.String),
            pl.col("statement_file_name").cast(pl.String),
            pl.col("tx_amount").cast(pl.Decimal),
            pl.col("country_code").cast(pl.String),
            pl.col("tx_date").str.strptime(pl.Date),
            pl.col("descriptions").cast(pl.List(pl.String)),
            pl.col("exchange_rate").cast(pl.Float32),
            pl.col("foreign_amount").cast(pl.Decimal),
            pl.col("foreign_currency").cast(pl.String),
        )

    def parse_file(file_path: Path):
        data_as_json = [
            transaction.as_json_like
            for transaction in icspdfparser.read_ics_pdf(file_path)
        ]
        df = pl.DataFrame(data_as_json).rename(
            mapping=IcsCreditStatementProcessor.file_to_table_columnn_map
        )

        # Polars way to replace empty strings with None
        df = df.with_columns(
            [
                pl.when(pl.col("foreign_amount") == "")
                .then(None)
                .otherwise(pl.col("foreign_amount"))
                .alias("foreign_amount"),
                pl.when(pl.col("foreign_currency") == "")
                .then(None)
                .otherwise(pl.col("foreign_currency"))
                .alias("foreign_currency"),
            ]
        )

        df = df.with_columns(
            [
                pl.lit(file_path.name).alias(
                    "statement_file_name"
                ),  # Add filename column
                pl.arange(0, len(df)).alias("statement_id_in_file"),  # Add ID column
            ]
        )
        df = IcsCreditStatementProcessor._map_dtypes(df)
        return df

    def get_update_database_query(file_path: Path, file_content: pl.DataFrame):
        columns = file_content.columns
        query = sql.SQL(
            "INSERT INTO {schema}.{table} ({columns}) VALUES ("
            + ",".join(["%s" for i in range(len(columns))])
            + ") ON CONFLICT (statement_file_name, statement_id_in_file) DO UPDATE SET {replaced_columns}"
        ).format(
            schema=sql.Identifier(TX_SCHEMA),
            table=sql.Identifier(CREDIT_CRD_TX_TABLE),
            columns=sql.SQL(", ").join(map(sql.Identifier, columns)),
            replaced_columns=sql.SQL(", ").join(
                sql.SQL("{} = EXCLUDED.{}").format(sql.Identifier(c), sql.Identifier(c))
                for c in columns
                if c
                not in (
                    "statement_file_name",
                    "statement_id_in_file",
                )  # Exclude primary key
            ),
        )
        return query
