import argparse
import logging
import sys
from nicegui import app, ui
from transaction_services.config.config_reader import Config, get_config
from transaction_services.ui.views.base_views_nicegui import BaseNiceGUIView

# Import views (placeholders for now, will be implemented next)
from transaction_services.ui.views.analysis_views_nicegui import ExpenditureGraphNiceGUI
from transaction_services.ui.views.cash_category_linking_nicegui import ManageCashCategoriesNiceGUI, DebitCashCategoryLinkingNiceGUI
from transaction_services.ui.views.loan_management_nicegui import ManageLoanEntriesNiceGUI
from transaction_services.ui.views.loan_linking_nicegui import DebitTxLoanLinkingNiceGUI
from transaction_services.ui.views.manual_and_dd_nicegui import ManageManualTxEntriesNiceGUI, DirectDebitLinkingNiceGUI

logger = logging.getLogger(__name__)
logging.basicConfig(stream=sys.stdout, encoding="utf-8", level=logging.INFO)

def create_arg_parser():
    parser = argparse.ArgumentParser(
        description="NiceGUI Dashboard for viewing Transaction data"
    )
    parser.add_argument(
        "--config-file", type=str, required=True, help="Path to the configuration file"
    )
    return parser.parse_args()

def get_config_from_args():
    args = create_arg_parser()
    config: Config = get_config(args.config_file)
    logger.info("Starting Finance Dashboard NiceGUI with config: %s", config)
    return config

def init_ui(config: Config):
    postgres_conn_str = config.postgres_conn_str
    
    # Define views with icons for better navigation UX
    available_views: list[BaseNiceGUIView] = [
        ExpenditureGraphNiceGUI(postgres_conn_str),
        ManageCashCategoriesNiceGUI(postgres_conn_str),
        DebitCashCategoryLinkingNiceGUI(postgres_conn_str),
        ManageLoanEntriesNiceGUI(postgres_conn_str),
        DebitTxLoanLinkingNiceGUI(postgres_conn_str),
        ManageManualTxEntriesNiceGUI(postgres_conn_str),
        DirectDebitLinkingNiceGUI(postgres_conn_str),
    ]
    
    view_dict = {view.view_name(): view for view in available_views}
    # Map view names to icons
    icon_map = {
        "Expenditure Graph": "analytics",
        "Manage Tx Categories": "category",
        "Cash Category link ABN transactions": "link",
        "Manage Loan Entries": "payments",
        "Loan link ABN transactions": "sync_alt",
        "Manage Manual Tx Entries": "edit_note",
        "Direct Debit Linking": "credit_card"
    }

    @ui.page('/')
    def main_page():
        # Serve static files
        import os
        static_dir = os.path.join(os.path.dirname(__file__), 'static')
        app.add_static_files('/static', static_dir)
        
        # --- Theme & Global Styles ---
        ui.colors(primary='#4f46e5', secondary='#10b981', accent='#6366f1', positive='#10b981', negative='#ef4444', info='#3b82f6')
        
        ui.add_head_html('''
            <link href="https://fonts.googleapis.com/css2?family=Outfit:wght@300;400;500;600;700&display=swap" rel="stylesheet">
            <link rel="stylesheet" href="/static/style.css?v=1.1">
        ''')

        with ui.header().classes('bg-white border-b border-slate-200 text-slate-900 px-6 py-4 items-center'):
            ui.button(on_click=lambda: drawer.toggle()).props('flat round icon=menu').classes('text-slate-600')
            with ui.row().classes('items-center'):
                ui.icon('account_balance', color='primary').classes('text-2xl')
                ui.label('FinanceFlow').classes('text-xl font-bold tracking-tight text-slate-800 ml-2')
            ui.space()
        
        with ui.left_drawer(fixed=True).classes('bg-white sidebar-shadow border-r border-slate-200 transition-all duration-300').props('width=320') as drawer:
            with ui.column().classes('w-full px-2 py-8'):
                ui.label('MAIN MENU').classes('text-xs font-semibold text-slate-400 tracking-widest mb-4 px-6')
                
                with ui.tabs().props('vertical indicator-color=transparent').classes('w-full text-slate-600') as tabs:
                    for name in sorted(view_dict.keys()):
                        icon = icon_map.get(name, 'article')
                        ui.tab(name, label=name, icon=icon).on('click', lambda: drawer.hide() if not drawer.props.get('fixed') else None)

        with ui.tab_panels(tabs, value=sorted(view_dict.keys())[0], animated=True).classes('w-full bg-transparent'):
            for name in sorted(view_dict.keys()):
                with ui.tab_panel(name).classes('p-0 bg-transparent'):
                    with ui.column().classes('w-full px-6 py-8 gap-8'):
                        view_dict[name].render(ui.column().classes('w-full'))

if __name__ in {"__main__", "__mp_main__"}:
    try:
        config = get_config_from_args()
        init_ui(config)
        ui.run(title='Finance Dashboard', port=8080)
    except Exception as e:
        logger.error(f"Failed to start NiceGUI dashboard: {e}")
        # When running via command line, we might not have args if we just run 'python ...'
        # But rye run will provide them if configured.
