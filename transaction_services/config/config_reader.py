import yaml
from pathlib import Path
from dataclasses import dataclass


@dataclass
class Config:
    postgres_conn_str: str
    debit_stmt_input_dir: Path
    credit_card_stmt_input_dir: Path


def _get_postgres_conn_str(postgres_conf: dict) -> str:
    postgres_conn_str = (
        "postgresql://{username}:{password}@{ipaddress}:{port}/{dbname}".format(
            username=postgres_conf["username"],
            password=postgres_conf["password"],
            ipaddress=postgres_conf["ip_address"],
            port=postgres_conf["port"],
            dbname=postgres_conf["db_name"],
        )
    )
    return postgres_conn_str


def get_config(config_file: Path) -> Config:
    parsed_yaml = None
    with open(config_file) as file:
        parsed_yaml = dict(yaml.safe_load(file))
    return Config(
        postgres_conn_str=_get_postgres_conn_str(postgres_conf=parsed_yaml["postgres"]),
        debit_stmt_input_dir=Path(
            parsed_yaml["statement_services"]["debit_stmt_input_dir"]
        ),
        credit_card_stmt_input_dir=Path(
            parsed_yaml["statement_services"]["credit_card_stmt_input_dir"]
        ),
    )
