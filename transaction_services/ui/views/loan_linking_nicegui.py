import psycopg2
import pandas as pd
import datetime
from nicegui import ui
from transaction_services.ui.views.base_views_nicegui import TimeRangeNiceGUIView
from transaction_services.config.db_constants import (
    TX_SCHEMA,
    TX_CATEGORY_TABLE,
    DEBIT_TX_TABLE,
    MANUAL_TX_TABLE,
    CREDIT_CRD_TX_TABLE,
    LOAN_TABLE,
)

class DebitTxLoanLinkingNiceGUI(TimeRangeNiceGUIView):
    def __init__(self, db_conn_str: str):
        super().__init__(db_conn_str=db_conn_str, months_of_history=3)

    def view_name(self) -> str:
        return "Loan link ABN transactions"

    def data_view(self, container: ui.element, start_date: datetime.date, end_date: datetime.date) -> None:
        container.clear()
        conn = psycopg2.connect(self.db_conn_str)
        cur = conn.cursor()

        # Fetch loans
        cur.execute(
            f"""SELECT id, tx_amount_borrowed, is_settlement, counterparty, remarks, tx_date, debit_tx_reference, currency, foreign_amt_borrowed
            FROM {TX_SCHEMA}.{LOAN_TABLE}
            WHERE tx_date >= '{start_date}' and tx_date <= '{end_date}'
            order by id desc"""
        )
        loan_data = cur.fetchall()
        loan_df = pd.DataFrame(loan_data, columns=[desc[0] for desc in cur.description])

        # Fetch transactions
        cur.execute(f"""
                select
                dt.id,
                dt.bank,
                dt.tx_amount,
                tc.category,
                tc.subcategory,
                dt.remarks,
                dt.recurrence,
                dt.description,
                dt.tx_date
            from
                {TX_SCHEMA}.{DEBIT_TX_TABLE} dt
            left join {TX_SCHEMA}.{TX_CATEGORY_TABLE} tc on
                dt.tx_category = tc.id
            where
                dt.tx_date >= '{start_date}' and dt.tx_date <='{end_date}'
            order by
                CASE WHEN tc.category IS NULL THEN 0 ELSE 1 END,
                dt.tx_date desc, dt.id desc
            """)
        tx_data = cur.fetchall()
        tx_df = pd.DataFrame(tx_data, columns=[desc[0] for desc in cur.description])
        cur.close()
        conn.close()

        with container:
            with ui.row().classes('w-full no-wrap q-gutter-md'):
                # Left side: Transactions
                with ui.column().classes('flex-grow'):
                    ui.label('ABN Transactions').classes('text-h6')
                    tx_grid = ui.aggrid({
                        'columnDefs': [
                            {'headerName': 'ID', 'field': 'id', 'checkboxSelection': True, 'width': 80},
                            {'headerName': 'Date', 'field': 'tx_date', 'width': 100},
                            {'headerName': 'Amount', 'field': 'tx_amount', 'width': 100},
                            {'headerName': 'Category', 'field': 'category', 'width': 120},
                            {'headerName': 'Description', 'field': 'description', 'width': 200},
                        ],
                        'rowData': tx_df.to_dict('records'),
                        'rowSelection': 'single',
                    }).classes('w-full').style('height: 600px')

                # Right side: Loans
                with ui.column().classes('flex-grow'):
                    ui.label('Existing Loans').classes('text-h6')
                    loan_grid = ui.aggrid({
                        'columnDefs': [
                            {'headerName': 'Amount', 'field': 'tx_amount_borrowed', 'checkboxSelection': True, 'width': 100},
                            {'headerName': 'Counterparty', 'field': 'counterparty', 'width': 120},
                            {'headerName': 'Date', 'field': 'tx_date', 'width': 100},
                        ],
                        'rowData': loan_df.to_dict('records'),
                        'rowSelection': 'multiple',
                    }).classes('w-full').style('height: 600px')

            ui.button('Link Loan', on_click=lambda: self.link_loan(tx_grid, loan_grid, container, start_date, end_date)).props('color=primary icon=link').classes('q-mt-md')

    async def link_loan(self, tx_grid, loan_grid, container, start_date, end_date):
        tx_rows = await tx_grid.get_selected_rows()
        loan_rows = await loan_grid.get_selected_rows()
        if not tx_rows or not loan_rows:
            ui.notify('Select one transaction and at least one loan', type='warning')
            return
        
        tx_id = tx_rows[0]['id']
        loan_ids = [row['id'] for row in loan_rows]
        
        # Validation: Dates must match (simplified check)
        tx_date = tx_rows[0]['tx_date']
        for row in loan_rows:
            if row['tx_date'] != tx_date:
                ui.notify(f"Date mismatch: Loan {row['id']} date {row['tx_date']} != TX date {tx_date}", type='negative')
                return

        conn = psycopg2.connect(self.db_conn_str)
        cur = conn.cursor()
        try:
            cur.execute(f"UPDATE {TX_SCHEMA}.{LOAN_TABLE} SET debit_tx_reference = %s WHERE id IN %s", (tx_id, tuple(loan_ids)))
            conn.commit()
            ui.notify('Linked successfully')
            self.update_data_view(container, start_date, end_date)
        except Exception as e:
            ui.notify(f'Error: {e}', type='negative')
        finally:
            cur.close()
            conn.close()
