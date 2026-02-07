import psycopg2
import pandas as pd
import datetime
from typing import Optional
from nicegui import ui
from transaction_services.ui.views.base_views_nicegui import BaseNiceGUIView, TimeRangeNiceGUIView
from transaction_services.config.db_constants import (
    TX_SCHEMA,
    TX_CATEGORY_TABLE,
    DEBIT_TX_TABLE,
    MANUAL_TX_TABLE,
    CREDIT_CRD_TX_TABLE,
)

class ManageCashCategoriesNiceGUI(BaseNiceGUIView):
    def __init__(self, db_conn_str: str):
        super().__init__(db_conn_str)

    def view_name(self) -> str:
        return "Manage Tx Categories"

    def render(self, container: ui.element) -> None:
        with container:
            ui.label(self.view_name()).classes('text-h4 q-mb-md')
            
            with ui.row().classes('items-center q-gutter-md q-mb-md'):
                ui.button('Refresh', on_click=lambda: self.update_table(table_container)).props('icon=refresh')

            table_container = ui.column().classes('w-full')
            self.update_table(table_container)

    def update_table(self, container: ui.element) -> None:
        container.clear()
        conn = psycopg2.connect(self.db_conn_str)
        cur = conn.cursor()
        cur.execute(f"SELECT id, category, subcategory FROM {TX_SCHEMA}.{TX_CATEGORY_TABLE} ORDER BY category, subcategory")
        data = cur.fetchall()
        df = pd.DataFrame(data, columns=[desc[0] for desc in cur.description])
        cur.close()
        conn.close()

        with container:
            grid = ui.aggrid({
                'columnDefs': [
                    {'headerName': 'ID', 'field': 'id', 'checkboxSelection': True, 'headerCheckboxSelection': True},
                    {'headerName': 'Category', 'field': 'category', 'editable': True},
                    {'headerName': 'Subcategory', 'field': 'subcategory', 'editable': True},
                ],
                'rowData': df.to_dict('records'),
                'rowSelection': 'multiple',
            }).classes('w-full').style('height: 400px')

            with ui.row().classes('q-mt-md'):
                ui.button('Delete Selected', on_click=lambda: self.delete_selected(grid)).props('color=negative')
                
                # Form for adding new category
                with ui.row().classes('items-center q-gutter-sm'):
                    cat_input = ui.input('Category')
                    subcat_input = ui.input('Subcategory')
                    ui.button('Add', on_click=lambda: self.add_category(cat_input.value, subcat_input.value, container))

    def delete_selected(self, grid: ui.aggrid) -> None:
        selected_rows = grid.get_selected_rows()
        if not selected_rows:
            ui.notify('No rows selected', type='warning')
            return
        
        ids_to_delete = [row['id'] for row in selected_rows]
        conn = psycopg2.connect(self.db_conn_str)
        cur = conn.cursor()
        try:
            cur.execute(f"DELETE FROM {TX_SCHEMA}.{TX_CATEGORY_TABLE} WHERE id IN %s", (tuple(ids_to_delete),))
            conn.commit()
            ui.notify(f'Deleted {len(ids_to_delete)} categories')
            self.update_table(grid.parent)
        except Exception as e:
            ui.notify(f'Error: {e}', type='negative')
        finally:
            cur.close()
            conn.close()

    def add_category(self, category: str, subcategory: str, container: ui.element) -> None:
        if not category or not subcategory:
            ui.notify('Category and Subcategory are required', type='warning')
            return
        
        conn = psycopg2.connect(self.db_conn_str)
        cur = conn.cursor()
        try:
            cur.execute(f"INSERT INTO {TX_SCHEMA}.{TX_CATEGORY_TABLE} (category, subcategory) VALUES (%s, %s)", (category, subcategory))
            conn.commit()
            ui.notify('Added category')
            self.update_table(container)
        except Exception as e:
            ui.notify(f'Error: {e}', type='negative')
        finally:
            cur.close()
            conn.close()

class DebitCashCategoryLinkingNiceGUI(TimeRangeNiceGUIView):
    def __init__(self, db_conn_str: str):
        super().__init__(db_conn_str=db_conn_str, months_of_history=3)

    def view_name(self) -> str:
        return "Cash Category link ABN transactions"

    def data_view(self, container: ui.element, start_date: datetime.date, end_date: datetime.date) -> None:
        conn = psycopg2.connect(self.db_conn_str)
        cur = conn.cursor()

        # Fetch categories
        cur.execute(f"SELECT id, category, subcategory FROM {TX_SCHEMA}.{TX_CATEGORY_TABLE} ORDER BY category, subcategory")
        cat_data = cur.fetchall()
        cat_df = pd.DataFrame(cat_data, columns=["id", "category", "subcategory"])

        # Fetch transactions
        cur.execute(f"""
                select
                dt.id,
                dt.bank,
                dt.account,
                dt.tx_amount,
                tc.category,
                tc.subcategory,
                dt.remarks,
                dt.recurrence,
                dt.description,
                dt.desc_json,
                dt.tx_date
            from
                {TX_SCHEMA}.{DEBIT_TX_TABLE} dt
            left join {TX_SCHEMA}.{TX_CATEGORY_TABLE} tc on
                dt.tx_category = tc.id
            where
                dt.tx_date >=  '{start_date}' and dt.tx_date <='{end_date}'
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
                    ui.label('Transactions').classes('text-h6')
                    tx_grid = ui.aggrid({
                        'columnDefs': [
                            {'headerName': 'ID', 'field': 'id', 'checkboxSelection': True, 'headerCheckboxSelection': True, 'width': 80},
                            {'headerName': 'Date', 'field': 'tx_date', 'width': 100},
                            {'headerName': 'Amount', 'field': 'tx_amount', 'width': 100},
                            {'headerName': 'Category', 'field': 'category', 'width': 120, 
                             'cellStyle': {'style': 'expression: params.value ? {} : {"backgroundColor": "indigo", "color": "white"}'}},
                            {'headerName': 'Subcategory', 'field': 'subcategory', 'width': 120},
                            {'headerName': 'Description', 'field': 'description', 'width': 200},
                        ],
                        'rowData': tx_df.to_dict('records'),
                        'rowSelection': 'multiple',
                    }).classes('w-full').style('height: 600px')

                # Right side: Categories
                with ui.column().classes('w-96'):
                    ui.label('CATEGORY DICTIONARY').classes('text-xs font-bold text-slate-400 tracking-widest mb-2')
                    cat_grid = ui.aggrid({
                        'columnDefs': [
                            {'headerName': 'Category', 'field': 'category', 'sortable': True, 'filter': True},
                            {'headerName': 'Subcategory', 'field': 'subcategory', 'sortable': True, 'filter': True},
                        ],
                        'rowData': cat_df.to_dict('records'),
                        'rowSelection': 'single',
                    }).classes('w-full shadow-sm rounded-lg overflow-hidden border border-slate-200').style('height: 600px')

            with ui.row().classes('mt-8 gap-6 items-end'):
                ui.button('Link Selection', on_click=lambda: self.link_category(tx_grid, cat_grid, container, start_date, end_date)).props('unelevated icon=link').classes('bg-indigo-600 text-white px-8 py-2 rounded-lg font-medium shadow-sm hover:bg-indigo-700 transition-colors h-10')
                
                with ui.row().classes('items-center gap-2'):
                    remarks_input = ui.input('Remarks').props('outlined dense').classes('w-48')
                    ui.button(on_click=lambda: self.set_field(tx_grid, 'remarks', remarks_input.value, container, start_date, end_date)).props('unelevated icon=save').classes('bg-slate-800 text-white p-2 rounded-lg hover:bg-slate-900 transition-colors h-10')
                
                with ui.row().classes('items-center gap-2'):
                    rec_select = ui.select(['Monthly', 'Yearly'], label='Recurrence').props('outlined dense').classes('w-32')
                    ui.button(on_click=lambda: self.set_field(tx_grid, 'recurrence', rec_select.value, container, start_date, end_date)).props('unelevated icon=repeat').classes('bg-slate-800 text-white p-2 rounded-lg hover:bg-slate-900 transition-colors h-10')

    def link_category(self, tx_grid, cat_grid, container, start_date, end_date):
        tx_rows = tx_grid.get_selected_rows()
        cat_rows = cat_grid.get_selected_rows()
        if not tx_rows or not cat_rows:
            ui.notify('Select both transactions and a category', type='warning')
            return
        
        tx_ids = [row['id'] for row in tx_rows]
        cat_id = cat_rows[0]['id']
        
        conn = psycopg2.connect(self.db_conn_str)
        cur = conn.cursor()
        try:
            cur.execute(f"UPDATE {TX_SCHEMA}.{DEBIT_TX_TABLE} SET tx_category = %s WHERE id IN %s", (cat_id, tuple(tx_ids)))
            conn.commit()
            ui.notify('Linked successfully')
            self.data_view(container, start_date, end_date)
        except Exception as e:
            ui.notify(f'Error: {e}', type='negative')
        finally:
            cur.close()
            conn.close()

    def set_field(self, tx_grid, field, value, container, start_date, end_date):
        tx_rows = tx_grid.get_selected_rows()
        if not tx_rows:
            ui.notify('No transactions selected', type='warning')
            return
        
        tx_ids = [row['id'] for row in tx_rows]
        conn = psycopg2.connect(self.db_conn_str)
        cur = conn.cursor()
        try:
            cur.execute(f"UPDATE {TX_SCHEMA}.{DEBIT_TX_TABLE} SET {field} = %s WHERE id IN %s", (value, tuple(tx_ids)))
            conn.commit()
            ui.notify(f'Updated {field}')
            self.data_view(container, start_date, end_date)
        except Exception as e:
            ui.notify(f'Error: {e}', type='negative')
        finally:
            cur.close()
            conn.close()
