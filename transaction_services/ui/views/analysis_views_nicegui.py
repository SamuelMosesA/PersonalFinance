import psycopg2
import pandas as pd
import datetime
import plotly.express as px
from nicegui import ui
from transaction_services.ui.views.base_views_nicegui import TimeRangeNiceGUIView
from transaction_services.config.db_constants import (
    TX_SCHEMA,
    TX_CATEGORY_TABLE,
    DEBIT_TX_TABLE,
    CREDIT_CRD_TX_TABLE,
    MANUAL_TX_TABLE,
    LOAN_TABLE,
)

class ExpenditureGraphNiceGUI(TimeRangeNiceGUIView):
    def __init__(self, db_conn_str: str):
        super().__init__(db_conn_str, 1)

    def view_name(self) -> str:
        return "Expenditure Graph"

    def data_view(self, container: ui.element, start_date: datetime.date, end_date: datetime.date) -> None:
        conn = psycopg2.connect(self.db_conn_str)
        cur = conn.cursor()

        # Fetch data (same query as original)
        query = f"""
            with loan_corrections as(
                select l.debit_tx_reference, sum(l.tx_amount_borrowed) as tx_amount_borrowed
                FROM {TX_SCHEMA}.{LOAN_TABLE} l
                where l.debit_tx_reference is not null 
                AND l.tx_date >= '{start_date}' AND l.tx_date <= '{end_date}'
                group by l.debit_tx_reference
            ),
            manual_corrections as(
            select mtx.correcting_debit_tx_ref
            FROM {TX_SCHEMA}.{MANUAL_TX_TABLE} mtx
            where mtx.correcting_debit_tx_ref is not null
            AND mtx.correcting_tx_date >= '{start_date}' and mtx.correcting_tx_date <= '{end_date}'
            ),
            credit_tx as(
                select 'credit' as source, 
                cdt.tx_amount as tx_amount,
                cdt.tx_category as tx_category_id,
                cdt.remarks as remarks,
                cdt.descriptions::text as descripton,
                cdt.tx_date as tx_date,
                cdt.statement_file_name || ', ' || cdt.statement_id_in_file::text as id
            from
                {TX_SCHEMA}.{CREDIT_CRD_TX_TABLE} cdt
            where
                cdt.direct_debit_link is null
            AND cdt.tx_date >= '{start_date}' AND cdt.tx_date <= '{end_date}'
            ),
            debit_tx as(
                select 
                'debit:' || dt.bank as source, 
                dt.tx_amount + COALESCE(-lc.tx_amount_borrowed,0) as tx_amount,
                dt.tx_category as tx_category_id,
                dt.remarks as remarks,
                CASE WHEN dt.bank = 'abn_current' THEN dt.desc_json::text ELSE dt.description END AS description,
                dt.tx_date as tx_date,
                dt.id::text as id
            FROM
                {TX_SCHEMA}.{DEBIT_TX_TABLE} dt
            LEFT JOIN loan_corrections lc ON
                dt.id = lc.debit_tx_reference
             WHERE
                NOT EXISTS (
                    SELECT 1
                    FROM {TX_SCHEMA}.{CREDIT_CRD_TX_TABLE} cdt
                    WHERE cdt.direct_debit_link = dt.id  
                    and cdt.tx_date >= '{start_date}'::date - interval '1 month' and cdt.tx_date <= '{end_date}'
                    )
                and
                NOT EXISTS (
                    SELECT 1
                    FROM manual_corrections mc
                    WHERE mc.correcting_debit_tx_ref = dt.id  
                    )
            AND dt.tx_date >= '{start_date}' AND dt.tx_date <= '{end_date}'
            ),
            manual_tx as(
                select 'manual' as source, 
                mdt.tx_amount as tx_amount,
                mdt.tx_category as tx_category_id,
                mdt.remarks as remarks,
                mdt.description as description,
                mdt.tx_date as tx_date,
                mdt.id::text as id
            from
                {TX_SCHEMA}.{MANUAL_TX_TABLE} mdt
            WHERE
              mdt.tx_date >= '{start_date}' AND mdt.tx_date <= '{end_date}'
            ),
            all_tx as(
                SELECT * FROM debit_tx
                UNION ALL
                SELECT * FROM credit_tx
                UNION ALL
                SELECT * FROM manual_tx
            ), 
            all_tx_with_category as(
                SELECT
                    at.tx_amount,
                    at.source,
                    at.id,
                    coalesce(tc.category, 'na') AS category,
                    coalesce(tc.subcategory, 'na') AS subcategory,
                    at.remarks,
                    at.description,
                    at.tx_date
                FROM
                    all_tx at
                LEFT JOIN
                    {TX_SCHEMA}.{TX_CATEGORY_TABLE} tc ON at.tx_category_id = tc.id
            )
            SELECT 
                -atxc.tx_amount as tx_amount,
                atxc.source,
                atxc.id,
                atxc.category,
                atxc.subcategory,
                atxc.remarks,
                atxc.description,
                atxc.tx_date
            FROM
                all_tx_with_category atxc
            WHERE
                atxc.category NOT IN ('Foreign Transfer')
            ORDER BY atxc.tx_amount, atxc.tx_date desc
            """
        cur.execute(query=query)
        all_exp_tx_entries = cur.fetchall()
        df = pd.DataFrame(all_exp_tx_entries, columns=[desc[0] for desc in cur.description])
        cur.close()
        conn.close()

        with container:
            if df.empty:
                ui.label('No data found for the selected time range.').classes('text-h6 text-grey q-mt-md')
                return

            total_flow = df['tx_amount'].sum()
            
            ui.notify(f"Total Flow: {total_flow:.2f}")
            
            with ui.row().classes('w-full gap-8 mb-8 justify-center'):
                with ui.element('div').classes('p-8 bg-white rounded-2xl border-l-4 border-indigo-500 shadow-sm flex flex-col items-center min-w-[320px]'):
                    ui.label('TOTAL FLOW').classes('text-xs font-bold text-slate-400 tracking-widest mb-2')
                    ui.label(f'€ {total_flow:,.2f}').classes('text-4xl font-extrabold text-slate-900')
                
                all_tx_cat_df = (
                    df.groupby(["category", "subcategory"])["tx_amount"]
                    .sum()
                    .reset_index()
                    .sort_values("tx_amount", ascending=False)
                )
                all_tx_exp_cat_df = all_tx_cat_df[all_tx_cat_df["tx_amount"] > 0]
                total_expenditure = all_tx_exp_cat_df["tx_amount"].sum()
                
                with ui.element('div').classes('p-8 bg-white rounded-2xl border-l-4 border-rose-500 shadow-sm flex flex-col items-center min-w-[320px]'):
                    ui.label('TOTAL EXPENDITURE').classes('text-xs font-bold text-slate-400 tracking-widest mb-2')
                    ui.label(f'€ {total_expenditure:,.2f}').classes('text-4xl font-extrabold text-slate-900')

            # Plotly Sunburst
            fig = px.sunburst(
                all_tx_exp_cat_df,
                path=["category", "subcategory"],
                values="tx_amount",
                color="tx_amount",
                color_continuous_scale="sunset",
            )
            fig.update_layout(height=800, margin=dict(t=20, l=0, r=0, b=20), paper_bgcolor='rgba(0,0,0,0)', plot_bgcolor='rgba(0,0,0,0)')
            ui.plotly(fig).classes('w-full').style('height: 800px')

            # Transaction Table (using AgGrid)
            ui.label('TRANSACTION LOG').classes('text-xs font-bold text-slate-400 tracking-widest mt-12 mb-4')
            
            # Prepare data for AgGrid
            grid_data = df.to_dict('records')
            # Format dates for display
            for row in grid_data:
                if isinstance(row['tx_date'], datetime.date):
                    row['tx_date'] = row['tx_date'].isoformat()

            ui.aggrid({
                'columnDefs': [
                    {'headerName': 'Date', 'field': 'tx_date', 'sortable': True, 'filter': True, 'width': 120},
                    {'headerName': 'Amount', 'field': 'tx_amount', 'sortable': True, 'filter': True, 'width': 120},
                    {'headerName': 'Source', 'field': 'source', 'sortable': True, 'filter': True, 'width': 120},
                    {'headerName': 'Category', 'field': 'category', 'sortable': True, 'filter': True},
                    {'headerName': 'Subcategory', 'field': 'subcategory', 'sortable': True, 'filter': True},
                    {'headerName': 'Remarks', 'field': 'remarks', 'sortable': True, 'filter': True},
                    {'headerName': 'Description', 'field': 'description', 'sortable': True, 'filter': True, 'width': 300},
                ],
                'rowData': grid_data,
            }).classes('w-full shadow-sm rounded-xl overflow-hidden border border-slate-200').style('height: 600px')

            # Category Summary Table
            ui.label('CATEGORY BREAKDOWN').classes('text-xs font-bold text-slate-400 tracking-widest mt-12 mb-4')
            ui.table(
                columns=[{'name': c, 'label': c, 'field': c, 'sortable': True} for c in all_tx_cat_df.columns],
                rows=all_tx_cat_df.to_dict('records'),
            ).classes('w-full shadow-sm border border-slate-200 rounded-xl overflow-hidden').props('flat bordered')

            # Download button
            with ui.row().classes('w-full justify-end mt-8'):
                def download():
                    csv_data = df.to_csv(index=False)
                    ui.download(csv_data.encode(), filename='transactions.csv')
                
                ui.button('Export to CSV', on_click=download).props('unelevated icon=download').classes('bg-slate-900 text-white px-6 py-2 rounded-lg font-medium hover:bg-slate-800 transition-colors')
