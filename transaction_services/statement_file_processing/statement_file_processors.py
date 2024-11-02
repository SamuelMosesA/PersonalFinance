from pathlib import Path
from .lib.base_statement_processor import BaseStatementProcessor
from .lib.abn_statement_processing import AbnStatementProcessor
from .lib.ics_credit_statement_processing import (
    IcsCreditStatementProcessor,
)
from transaction_services.config.config_reader import get_config, Config
import psycopg2
import argparse



def process_file(
    processor: BaseStatementProcessor.__class__,
    file_path: Path,
    db_conn: psycopg2.extensions.connection,
) -> None:
    data = processor.parse_file(file_path)
    print(f"File: {file_path}")
    print(data)
    query = processor.get_update_database_query(file_path, data)
    with db_conn.cursor() as cur:
        cur.executemany(query, data.rows())
        db_conn.commit()
    return


def delegate_new_files_to_processor(
    config: Config,
    db_conn: psycopg2.extensions.connection
) -> None:
    abn_debit_transactions_folder = config.debit_stmt_input_dir
    for file_path in abn_debit_transactions_folder.glob("*.TAB"):
        process_file(AbnStatementProcessor, file_path, db_conn)
        file_path.rename(file_path.parent / (file_path.name + ".success"))
    ic_credit_transactions_foler = config.credit_card_stmt_input_dir
    for file_path in ic_credit_transactions_foler.glob("Statement-*.pdf"):
        process_file(IcsCreditStatementProcessor, file_path, db_conn)
        file_path.rename(file_path.parent / (file_path.name + ".success"))


def create_arg_parser():
    parser = argparse.ArgumentParser(description='Process bank statement files and store data in a database.')
    parser.add_argument('--config-file', type=str, required=True, help='Path to the configuration file')
    return parser.parse_args()

def main() -> None:
    args = create_arg_parser()
    config = get_config(args.config_file)
    db_conn = psycopg2.connect(
        config.postgres_conn_str
    )
    delegate_new_files_to_processor(config, db_conn)
    db_conn.close()
