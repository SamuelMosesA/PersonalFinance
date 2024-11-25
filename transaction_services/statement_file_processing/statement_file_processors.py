from pathlib import Path
from .lib.base_statement_processor import BaseStatementProcessor
from .lib.abn_statement_processing import AbnStatementProcessor
from .lib.bunq_statement_processing import BunqStatementProcessor
from .lib.ics_credit_statement_processing import (
    IcsCreditStatementProcessor,
)
from transaction_services.config.config_reader import (
    get_config,
    Config,
    StmtInputFileConfig,
)
import psycopg2
import argparse
import logging
import sys
from time import sleep

logger = logging.getLogger(__name__)
logging.basicConfig(stream=sys.stdout, encoding="utf-8", level=logging.INFO)


def process_file(
    processor: BaseStatementProcessor.__class__,
    file_path: Path,
    db_conn: psycopg2.extensions.connection,
) -> bool:
    try:
        data = processor.parse_file(file_path)
        logger.info("Processing file: %s", file_path)
        logger.info("Data:\n%s", data)
        query = processor.get_update_database_query(file_path, data)
        with db_conn.cursor() as cur:
            cur.executemany(query, data.rows())
            db_conn.commit()
    except Exception as e:
        logger.exception(e)
        return False
    return True


StmtProcessorConfig = dict[BaseStatementProcessor.__class__, StmtInputFileConfig]


def get_processor_config(config: Config) -> StmtProcessorConfig:
    return {
        AbnStatementProcessor: config.abn_stmt_input,
        BunqStatementProcessor: config.bunq_stmt_input,
        IcsCreditStatementProcessor: config.credit_card_stmt_input,
    }


def delegate_new_files_to_processor(
    processor_config: StmtProcessorConfig, db_conn: psycopg2.extensions.connection
) -> None:
    for processor_class, input_file_conf in processor_config.items():
        for file_path in input_file_conf.input_dir.glob(input_file_conf.file_glob):
            is_success = process_file(processor_class, file_path, db_conn)
            if is_success:
                file_path.rename(file_path.parent / (file_path.name + ".success"))


def create_arg_parser():
    parser = argparse.ArgumentParser(
        description="Process bank statement files and store data in a database."
    )
    parser.add_argument(
        "--config-file", type=str, required=True, help="Path to the configuration file"
    )
    return parser.parse_args()


def main() -> None:
    args = create_arg_parser()
    config = get_config(args.config_file)
    logger.info("Starting statement file processor with config: %s", config)
    processor_config = get_processor_config(config=config)
    while True:
        db_conn = psycopg2.connect(config.postgres_conn_str)
        logger.info("Searching files")
        delegate_new_files_to_processor(processor_config, db_conn)
        sleep(60)
        db_conn.close()
