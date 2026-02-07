import psycopg2
import pandas as pd
import datetime
from nicegui import ui
from transaction_services.ui.views.base_views_nicegui import TimeRangeNiceGUIView
from transaction_services.config.db_constants import (
    TX_SCHEMA,
    LOAN_TABLE,
)

class ManageLoanEntriesNiceGUI(TimeRangeNiceGUIView):
    def __init__(self, db_conn_str: str):
        super().__init__(db_conn_str=db_conn_str, months_of_history=3)

    def view_name(self) -> str:
        return "Manage Loan Entries"

    def data_view(self, container: ui.element, start_date: datetime.date, end_date: datetime.date) -> None:
        conn = psycopg2.connect(self.db_conn_str)
        cur = conn.cursor()

        # Fetch data
        cur.execute(
            f"""SELECT id, tx_amount_borrowed, counterparty, remarks, tx_date, currency, foreign_amt_borrowed, settling_loan_tx_link, is_settlement, debit_tx_reference
            FROM {TX_SCHEMA}.{LOAN_TABLE}
            WHERE tx_date >= '{start_date}' and tx_date <= '{end_date}'
            order by id desc"""
        )
        data = cur.fetchall()
        df = pd.DataFrame(data, columns=[desc[0] for desc in cur.description])
        cur.close()
        conn.close()

        with container:
            ui.label('LOAN PORTFOLIO').classes('text-xs font-bold text-slate-400 tracking-widest mb-4')
            
            grid = ui.aggrid({
                'columnDefs': [
                    {'headerName': 'ID', 'field': 'id', 'checkboxSelection': True, 'headerCheckboxSelection': True, 'width': 80},
                    {'headerName': 'Amount', 'field': 'tx_amount_borrowed', 'width': 120, 'sortable': True},
                    {'headerName': 'Counterparty', 'field': 'counterparty', 'width': 150, 'sortable': True, 'filter': True},
                    {'headerName': 'Remarks', 'field': 'remarks', 'width': 150},
                    {'headerName': 'Date', 'field': 'tx_date', 'width': 120, 'sortable': True},
                    {'headerName': 'Currency', 'field': 'currency', 'width': 100},
                    {'headerName': 'Settled?', 'field': 'is_settlement', 'width': 100},
                ],
                'rowData': df.to_dict('records'),
                'rowSelection': 'multiple',
            }).classes('w-full shadow-sm rounded-xl overflow-hidden border border-slate-200').style('height: 400px')

            with ui.row().classes('mt-6 gap-6 items-end'):
                ui.button('Delete Selected', on_click=lambda: self.delete_loans(grid, container, start_date, end_date)).props('unelevated icon=delete').classes('bg-rose-600 text-white px-8 py-2 rounded-lg font-medium shadow-sm hover:bg-rose-700 transition-colors h-10')
                
                with ui.row().classes('items-center gap-2 items-end'):
                    settlement_date = ui.input('Settlement Date').props('outlined dense').classes('w-48')
                    with settlement_date.add_slot('append'):
                        ui.icon('edit_calendar').on('click', lambda: menu.open()).classes('cursor-pointer text-indigo-500')
                    with ui.menu() as menu:
                        ui.date().bind_value(settlement_date)
                    settlement_date.value = datetime.date.today().isoformat()
                    
                    ui.button('Create Settlement', on_click=lambda: self.create_settlement(grid, settlement_date.value, container, start_date, end_date)).props('unelevated icon=handshake').classes('bg-indigo-600 text-white px-6 py-2 rounded-lg font-medium shadow-sm hover:bg-indigo-700 transition-colors h-10')

            # Add new rows form
            ui.label('ADD NEW LOAN ENTRY').classes('text-xs font-bold text-slate-400 tracking-widest mt-12 mb-4')
            
            with ui.row().classes('w-full items-end gap-4 p-6 bg-white border border-slate-200 rounded-xl shadow-sm'):
                amount = ui.number('Amount', value=0.0).props('outlined dense').classes('w-32')
                cp = ui.input('Counterparty').props('outlined dense').classes('flex-grow')
                rem = ui.input('Remarks').props('outlined dense').classes('flex-grow')
                curr = ui.input('Currency', value='EUR').props('outlined dense').classes('w-24')
                with ui.input('Date', value=datetime.date.today().isoformat()).props('outlined dense').classes('w-40') as dt_input:
                    with dt_input.add_slot('append'):
                        ui.icon('calendar_today').on('click', lambda: dt_menu.open()).classes('cursor-pointer text-indigo-500')
                    with ui.menu() as dt_menu:
                        ui.date().bind_value(dt_input)
                
                ui.button('Add Loan', on_click=lambda: self.add_loan(amount.value, cp.value, rem.value, curr.value, dt_input.value, container, start_date, end_date)).props('unelevated icon=add').classes('bg-slate-900 text-white px-8 py-2 rounded-lg font-medium hover:bg-slate-800 transition-colors h-10')

    def delete_loans(self, grid, container, start_date, end_date):
        rows = grid.get_selected_rows()
        if not rows:
            ui.notify('No rows selected', type='warning')
            return
        
        ids = [row['id'] for row in rows]
        conn = psycopg2.connect(self.db_conn_str)
        cur = conn.cursor()
        try:
            cur.execute(f"DELETE FROM {TX_SCHEMA}.{LOAN_TABLE} WHERE id IN %s", (tuple(ids),))
            conn.commit()
            ui.notify(f'Deleted {len(ids)} loans')
            self.data_view(container, start_date, end_date)
        except Exception as e:
            ui.notify(f'Error: {e}', type='negative')
        finally:
            cur.close()
            conn.close()

    def create_settlement(self, grid, settlement_date, container, start_date, end_date):
        rows = grid.get_selected_rows()
        if not rows:
            ui.notify('No loans selected for settlement', type='warning')
            return
        
        total_amt = sum(row['tx_amount_borrowed'] for row in rows)
        counterparties = ",".join(set(row['counterparty'] for row in rows))
        currencies = set(row['currency'] for row in rows)
        
        if len(currencies) > 1:
            ui.notify(f'Mixed currencies: {currencies}', type='negative')
            return
        
        currency = currencies.pop()
        ids = [row['id'] for row in rows]
        
        conn = psycopg2.connect(self.db_conn_str)
        cur = conn.cursor()
        try:
            cur.execute(
                f"""INSERT INTO {TX_SCHEMA}.{LOAN_TABLE} (tx_amount_borrowed, counterparty, remarks, currency, tx_date, is_settlement) 
                VALUES (%s, %s, %s, %s, %s, %s) 
                RETURNING id""",
                (-total_amt, counterparties, "Loan settlement", currency, settlement_date, True)
            )
            settlement_id = cur.fetchone()[0]
            cur.execute(
                f"UPDATE {TX_SCHEMA}.{LOAN_TABLE} SET settling_loan_tx_link = %s WHERE id IN %s",
                (settlement_id, tuple(ids))
            )
            conn.commit()
            ui.notify('Settlement created')
            self.data_view(container, start_date, end_date)
        except Exception as e:
            ui.notify(f'Error: {e}', type='negative')
        finally:
            cur.close()
            conn.close()

    def add_loan(self, amount, cp, remarks, currency, date, container, start_date, end_date):
        if not amount or not cp:
            ui.notify('Amount and Counterparty are required', type='warning')
            return
        
        conn = psycopg2.connect(self.db_conn_str)
        cur = conn.cursor()
        try:
            cur.execute(
                f"INSERT INTO {TX_SCHEMA}.{LOAN_TABLE} (tx_amount_borrowed, counterparty, remarks, currency, tx_date) VALUES (%s, %s, %s, %s, %s)",
                (amount, cp, remarks, currency, date)
            )
            conn.commit()
            ui.notify('Loan added')
            self.data_view(container, start_date, end_date)
        except Exception as e:
            ui.notify(f'Error: {e}', type='negative')
        finally:
            cur.close()
            conn.close()
