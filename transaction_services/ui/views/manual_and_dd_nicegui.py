import psycopg2
import pandas as pd
import datetime
import json
from nicegui import ui
from transaction_services.ui.views.base_views_nicegui import TimeRangeNiceGUIView
from transaction_services.config.db_constants import (
    TX_SCHEMA,
    MANUAL_TX_TABLE,
    DEBIT_TX_TABLE,
    TX_CATEGORY_TABLE,
    CREDIT_CRD_TX_TABLE,
)

class ManageManualTxEntriesNiceGUI(TimeRangeNiceGUIView):
    def __init__(self, db_conn_str: str):
        super().__init__(db_conn_str=db_conn_str, months_of_history=3)

    def view_name(self) -> str:
        return "Manage Manual Tx Entries"

    def data_view(self, container: ui.element, start_date: datetime.date, end_date: datetime.date) -> None:
        container.clear()
        conn = psycopg2.connect(self.db_conn_str)
        cur = conn.cursor()
        cur.execute(
            f"""SELECT id, tx_amount, currency, description, tx_date, remarks 
            FROM {TX_SCHEMA}.{MANUAL_TX_TABLE} 
            WHERE tx_date >= '{start_date}' and tx_date <= '{end_date}'
            order by id desc"""
        )
        data = cur.fetchall()
        df = pd.DataFrame(data, columns=[desc[0] for desc in cur.description])
        cur.close()
        conn.close()

        with container:
            ui.label('MANUAL JOURNAL ENTRIES').classes('text-xs font-bold text-slate-400 tracking-widest mb-4')
            grid = ui.aggrid({
                'columnDefs': [
                    {'headerName': 'ID', 'field': 'id', 'checkboxSelection': True, 'width': 80},
                    {'headerName': 'Amount', 'field': 'tx_amount', 'width': 120, 'sortable': True},
                    {'headerName': 'Date', 'field': 'tx_date', 'width': 120, 'sortable': True},
                    {'headerName': 'Currency', 'field': 'currency', 'width': 100},
                    {'headerName': 'Description', 'field': 'description', 'width': 300, 'filter': True},
                    {'headerName': 'Remarks', 'field': 'remarks', 'width': 200},
                ],
                'rowData': df.to_dict('records'),
                'rowSelection': 'multiple',
            }).classes('w-full shadow-sm rounded-xl overflow-hidden border border-slate-200').style('height: 400px')

            ui.button('Delete Selected', on_click=lambda: self.delete_manual_tx(grid, container, start_date, end_date)).props('unelevated icon=delete').classes('bg-rose-600 text-white px-8 py-2 rounded-lg font-medium shadow-sm hover:bg-rose-700 transition-colors mt-6')

            ui.label('ADD MANUAL TRANSACTION').classes('text-xs font-bold text-slate-400 tracking-widest mt-12 mb-4')
            with ui.row().classes('w-full items-end gap-4 p-6 bg-white border border-slate-200 rounded-xl shadow-sm'):
                amount = ui.number('Amount', value=0.0).props('outlined dense').classes('w-32')
                with ui.input('Date', value=datetime.date.today().isoformat()).props('outlined dense').classes('w-40') as dt_input:
                    with dt_input.add_slot('append'):
                        ui.icon('calendar_today').on('click', lambda: dt_menu.open()).classes('cursor-pointer text-indigo-500')
                    with ui.menu() as dt_menu:
                        ui.date().bind_value(dt_input)
                desc = ui.input('Description').props('outlined dense').classes('flex-grow')
                rem = ui.input('Remarks').props('outlined dense').classes('flex-grow')
                curr = ui.input('Currency', value='EUR').props('outlined dense').classes('w-24')
                ui.button('Add Entry', on_click=lambda: self.add_manual_tx(amount.value, dt_input.value, desc.value, rem.value, curr.value, container, start_date, end_date)).props('unelevated icon=add').classes('bg-slate-900 text-white px-8 py-2 rounded-lg font-medium hover:bg-slate-800 transition-colors h-10')

    async def delete_manual_tx(self, grid, container, start_date, end_date):
        rows = await grid.get_selected_rows()
        if not rows:
            ui.notify('No rows selected', type='warning')
            return
        ids = [row['id'] for row in rows]
        conn = psycopg2.connect(self.db_conn_str)
        cur = conn.cursor()
        try:
            cur.execute(f"DELETE FROM {TX_SCHEMA}.{MANUAL_TX_TABLE} WHERE id IN %s", (tuple(ids),))
            conn.commit()
            ui.notify(f'Deleted {len(ids)} entries')
            self.update_data_view(container, start_date, end_date)
        except Exception as e:
            ui.notify(f'Error: {e}', type='negative')
        finally:
            cur.close()
            conn.close()

    def add_manual_tx(self, amount, date, description, remarks, currency, container, start_date, end_date):
        if not amount or not description:
            ui.notify('Amount and Description are required', type='warning')
            return
        conn = psycopg2.connect(self.db_conn_str)
        cur = conn.cursor()
        try:
            cur.execute(
                f"INSERT INTO {TX_SCHEMA}.{MANUAL_TX_TABLE} (tx_amount, tx_date, description, remarks, currency) VALUES (%s, %s, %s, %s, %s)",
                (amount, date, description, remarks, currency)
            )
            conn.commit()
            ui.notify('Entry added')
            self.update_data_view(container, start_date, end_date)
        except Exception as e:
            ui.notify(f'Error: {e}', type='negative')
        finally:
            cur.close()
            conn.close()

class DirectDebitLinkingNiceGUI(TimeRangeNiceGUIView):
    def __init__(self, db_conn_str: str):
        super().__init__(db_conn_str=db_conn_str, months_of_history=3)

    def view_name(self) -> str:
        return "Direct Debit Linking"

    def data_view(self, container: ui.element, start_date: datetime.date, end_date: datetime.date) -> None:
        container.clear()
        conn = psycopg2.connect(self.db_conn_str)
        cur = conn.cursor()

        # Fetch Credit Card Transactions
        cur.execute(f"""
                select
                cdt.statement_id_in_file,
                cdt.statement_file_name,
                cdt.tx_amount,
                tc.category,
                tc.subcategory,
                cdt.remarks,
                cdt.tx_date
            from
                {TX_SCHEMA}.{CREDIT_CRD_TX_TABLE} cdt
            left join {TX_SCHEMA}.{TX_CATEGORY_TABLE} tc on
                cdt.tx_category = tc.id
            where
                cdt.tx_date >= '{start_date}' and cdt.tx_date <= '{end_date}'
                AND cdt.direct_debit_link is null
            order by
                CASE WHEN tc.category IS NULL THEN 0 ELSE 1 END,
                cdt.tx_date desc, cdt.statement_file_name, cdt.statement_id_in_file desc
            """)
        cc_data = cur.fetchall()
        cc_df = pd.DataFrame(cc_data, columns=[desc[0] for desc in cur.description])

        # Fetch ABN Transactions
        cur.execute(f"""
                select
                dt.id,
                dt.tx_amount,
                tc.category,
                dt.description,
                dt.tx_date
            from
                {TX_SCHEMA}.{DEBIT_TX_TABLE} dt
            left join {TX_SCHEMA}.{TX_CATEGORY_TABLE} tc on
                dt.tx_category = tc.id
            where
                dt.tx_date >=  '{start_date}' and dt.tx_date <='{end_date}'
                and dt.description ilike '%%INT CARD SERVICES%%'
            order by
                CASE WHEN tc.category IS NULL THEN 0 ELSE 1 END,
                dt.tx_date desc, dt.id desc
            """)
        abn_data = cur.fetchall()
        abn_df = pd.DataFrame(abn_data, columns=[desc[0] for desc in cur.description])
        cur.close()
        conn.close()

    async def link_direct_debit(self, abn_grid, cc_grid, container, start_date, end_date):
        abn_rows = await abn_grid.get_selected_rows()
        cc_rows = await cc_grid.get_selected_rows()
        if not abn_rows or not cc_rows:
            ui.notify('Select one ABN transaction and one Credit Card transaction', type='warning')
            return
        
        if abn_rows[0]['tx_amount'] + cc_rows[0]['tx_amount'] != 0:
            ui.notify('Amounts do not match!', type='negative')
            return
        
        abn_id = abn_rows[0]['id']
        cc_file = cc_rows[0]['statement_file_name']
        cc_id = cc_rows[0]['statement_id_in_file']

        conn = psycopg2.connect(self.db_conn_str)
        cur = conn.cursor()
        try:
            cur.execute(
                f"UPDATE {TX_SCHEMA}.{CREDIT_CRD_TX_TABLE} SET direct_debit_link = %s WHERE (statement_file_name, statement_id_in_file) = (%s, %s)",
                (abn_id, cc_file, cc_id)
            )
            conn.commit()
            ui.notify('Linked successfully')
            self.update_data_view(container, start_date, end_date)
        except Exception as e:
            ui.notify(f'Error: {e}', type='negative')
        finally:
            cur.close()
            conn.close()
