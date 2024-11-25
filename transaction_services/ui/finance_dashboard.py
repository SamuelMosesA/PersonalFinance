import streamlit as st
from transaction_services.ui.views.base_views import BaseStreamlitView
from transaction_services.ui.views.cash_category_linking import (
    ManageCashCategories,
    DebitCashCategoryLinking,
    CreditCrdCashCategoryLinking,
    ManualTxCashCategoryLinking,
)
from transaction_services.ui.views.loan_linking import (
    DebitTxLoanLinking,
    ManualTxLoanLinking,
    CreditCrdLoanLinking,
)

from transaction_services.ui.views.loan_managment import ManageLoanEntries

from transaction_services.ui.views.analysis_views import ExpenditureGraph
from transaction_services.ui.views.manual_transaction_management import (
    ManageManualTxEntries,
    CorrectDebitTx,
)
from transaction_services.ui.views.direct_debit_linking import DirectDebitLinking
from transaction_services.config.config_reader import Config, get_config
import argparse
import logging
import sys


logger = logging.getLogger(__name__)
logging.basicConfig(stream=sys.stdout, encoding="utf-8", level=logging.INFO)


def _get_views_dictionary(
    available_views: list[BaseStreamlitView],
) -> dict[str, BaseStreamlitView]:
    view_dict = {}
    for view in available_views:
        view_dict[view.view_name()] = view
    return view_dict


def render_all_views(available_views: list[BaseStreamlitView]):
    st.title("Personal Finance")
    # Searchable grid of views
    view_dict = _get_views_dictionary(available_views)
    selected_view = st.selectbox(
        "Select a view:", [str(k) for k in sorted(view_dict.keys())]
    )
    view_dict[selected_view].render()


st.set_page_config(
    page_title="Finance Dashboard",
    page_icon=None,
    layout="wide",
    initial_sidebar_state="auto",
    menu_items=None,
)


def create_arg_parser():
    parser = argparse.ArgumentParser(
        description="Dashboard for viewing Transaction data"
    )
    parser.add_argument(
        "--config-file", type=str, required=True, help="Path to the configuration file"
    )
    return parser.parse_args()


@st.cache_data
def get_cached_config():
    args = create_arg_parser()
    config: Config = get_config(args.config_file)
    logger.info("Starting Finance Dashboard with config: %s", config)
    return config


def main():
    postgres_conn_str = get_cached_config().postgres_conn_str
    available_views: list[BaseStreamlitView] = [
        ExpenditureGraph(postgres_conn_str),
        ManageCashCategories(postgres_conn_str),
        DebitCashCategoryLinking(postgres_conn_str),
        CreditCrdCashCategoryLinking(postgres_conn_str),
        ManualTxCashCategoryLinking(postgres_conn_str),
        ManageLoanEntries(postgres_conn_str),
        ManualTxLoanLinking(postgres_conn_str),
        DebitTxLoanLinking(postgres_conn_str),
        CreditCrdLoanLinking(postgres_conn_str),
        ManageManualTxEntries(postgres_conn_str),
        DirectDebitLinking(postgres_conn_str),
        CorrectDebitTx(postgres_conn_str),
    ]
    render_all_views(available_views)


main()
