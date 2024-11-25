import yaml
from pathlib import Path
from dataclasses import dataclass


@dataclass
class StmtInputFileConfig:
    input_dir: Path
    file_glob: str


@dataclass(frozen=True)
class Config:
    postgres_conn_str: str
    abn_stmt_input: StmtInputFileConfig
    credit_card_stmt_input: StmtInputFileConfig
    bunq_stmt_input: StmtInputFileConfig


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


def _get_stmt_input_file_config(
    stmt_input_file_config_dict: dict,
) -> StmtInputFileConfig:
    return StmtInputFileConfig(
        input_dir=Path(stmt_input_file_config_dict["input_dir"]),
        file_glob=stmt_input_file_config_dict["file_glob"],
    )


def get_config(config_file: Path) -> Config:
    parsed_yaml = None
    with open(config_file) as file:
        parsed_yaml = dict(yaml.safe_load(file))
    return Config(
        postgres_conn_str=_get_postgres_conn_str(postgres_conf=parsed_yaml["postgres"]),
        abn_stmt_input=_get_stmt_input_file_config(
            parsed_yaml["statement_services"]["abn_debit_stmt"]
        ),
        credit_card_stmt_input=_get_stmt_input_file_config(
            parsed_yaml["statement_services"]["credit_card_stmt"]
        ),
        bunq_stmt_input=_get_stmt_input_file_config(
            parsed_yaml["statement_services"]["bunq_stmt"]
        ),
    )
