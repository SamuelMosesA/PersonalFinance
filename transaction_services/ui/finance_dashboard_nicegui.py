import argparse
import logging
import sys
from nicegui import ui
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
        # --- Theme & Global Styles ---
        ui.colors(primary='#4f46e5', secondary='#10b981', accent='#6366f1', positive='#10b981', negative='#ef4444', info='#3b82f6')
        
        ui.add_head_html('''
            <link href="https://fonts.googleapis.com/css2?family=Outfit:wght@300;400;500;600;700&display=swap" rel="stylesheet">
            <style>
                body { font-family: 'Outfit', sans-serif !important; @apply bg-slate-50; }
                .nav-item { @apply rounded-lg mx-2 my-1 transition-all duration-200; }
                .nav-item:hover { @apply bg-indigo-50 translate-x-1; }
                .nav-item.active { @apply bg-indigo-600 text-white shadow-md; }
                .glass-card { @apply bg-white/80 backdrop-blur-md rounded-xl border border-white/20 shadow-sm; }
                .sidebar-shadow { box-shadow: 4px 0 15px -3px rgba(0, 0, 0, 0.05); }
            </style>
        ''')

        with ui.header().classes('bg-white border-b border-slate-200 text-slate-900 px-6 py-4 items-center'):
            ui.button(on_click=lambda: drawer.toggle()).props('flat round icon=menu').classes('text-slate-600')
            with ui.row().classes('items-center'):
                ui.icon('account_balance', color='primary').classes('text-2xl')
                ui.label('FinanceFlow').classes('text-xl font-bold tracking-tight text-slate-800 ml-2')
            ui.space()
        
        with ui.left_drawer(fixed=True).classes('bg-white sidebar-shadow border-r border-slate-200 transition-all duration-300') as drawer:
            with ui.column().classes('w-full px-4 py-8'):
                ui.label('MAIN MENU').classes('text-xs font-semibold text-slate-400 tracking-widest mb-4 px-4')
                
                with ui.list().classes('w-full gap-1'):
                    for name in sorted(view_dict.keys()):
                        icon = icon_map.get(name, 'article')
                        with ui.item(on_click=lambda n=name: select_view(n)).classes('nav-item cursor-pointer py-3 group'):
                            with ui.item_section().props('side'):
                                ui.icon(icon).classes('group-hover:text-indigo-600 transition-colors')
                            with ui.item_section():
                                ui.label(name).classes('text-sm font-medium text-slate-600 group-hover:text-slate-900 transition-colors')

        main_content = ui.column().classes('w-full px-6 py-8 gap-8')
        
        def select_view(name: str):
            main_content.clear()
            view_dict[name].render(main_content)
            # Only hide drawer on selection if it's in overlay mode (typical for mobile)
            if drawer.value and not drawer.props['fixed']: 
                drawer.hide()

        # Initial view
        if available_views:
            select_view(sorted(view_dict.keys())[0])
        else:
            with main_content:
                ui.label('No views available. Please implement and add views to the lists.').classes('text-h5 text-grey q-mt-xl')

if __name__ in {"__main__", "__mp_main__"}:
    try:
        config = get_config_from_args()
        init_ui(config)
        ui.run(title='Finance Dashboard', port=8080)
    except Exception as e:
        logger.error(f"Failed to start NiceGUI dashboard: {e}")
        # When running via command line, we might not have args if we just run 'python ...'
        # But rye run will provide them if configured.
