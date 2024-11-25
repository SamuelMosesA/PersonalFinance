from .base_statement_processor import BaseStatementProcessor
from psycopg2 import sql
import polars as pl
from pathlib import Path
import mt940
import json
from transaction_services.config.db_constants import TX_SCHEMA, DEBIT_TX_TABLE


class BunqStatementProcessor(BaseStatementProcessor):
    def parse_file(file_path: Path):
        data_dict = {
            "tx_date": [],
            "tx_amount": [],
            "start_balance": [],
            "end_balance": [],
            "account": [],
            "currency": [],
            "description": [],
            "desc_json": [],
            "bank": [],
        }
        bunq_data_parsed = mt940.parse(file_path)

        account = (
            bunq_data_parsed.data["account_identification"]
            + ":"
            + bunq_data_parsed.data["transaction_reference"]
        )

        start_balance = bunq_data_parsed.data["final_opening_balance"].amount.amount
        end_balance = None

        for transaction in bunq_data_parsed.transactions:
            tx_data = transaction.data
            tx_amount = tx_data["amount"].amount
            currency = tx_data["amount"].currency
            end_balance = start_balance + tx_amount
            tx_date = tx_data["entry_date"]
            description = tx_data["transaction_details"]
            desc_json = {
                "bank": "Bunq",
                "status": tx_data["status"],
                "id": tx_data["id"],
                "customer_reference": tx_data["customer_reference"],
                "bank_reference": tx_data["bank_reference"],
                "extra_details": tx_data["extra_details"],
                "currency": tx_data["currency"],
                "date": str(tx_data["date"]),
                "guessed_entry_date": str(tx_data["guessed_entry_date"]),
                "transaction_reference": tx_data["transaction_reference"],
            }
            data_dict["tx_date"].append(tx_date)
            data_dict["tx_amount"].append(tx_amount)
            data_dict["start_balance"].append(start_balance)
            data_dict["end_balance"].append(end_balance)
            data_dict["account"].append(account)
            data_dict["currency"].append(currency)
            data_dict["description"].append(description)
            data_dict["desc_json"].append(desc_json)
            data_dict["bank"].append("bunq")

            start_balance = end_balance

        assert (
            end_balance == bunq_data_parsed.data["final_closing_balance"].amount.amount
        )
        df = pl.DataFrame(data_dict).with_columns(
            pl.col("desc_json").map_elements(json.dumps, return_dtype=pl.String)
        )
        return df

    def get_update_database_query(file_path: Path, file_content: pl.DataFrame):
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
