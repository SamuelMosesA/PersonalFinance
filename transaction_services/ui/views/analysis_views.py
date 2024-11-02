import psycopg2
import pandas as pd
import streamlit as st
from .base_views import TimeRangeView
import datetime
import dateutil.relativedelta
import plotly.express as px
from . import (TX_SCHEMA, TX_CATEGORY_TABLE, DEBIT_TX_TABLE, CREDIT_CRD_TX_TABLE, MANUAL_TX_TABLE, LOAN_TABLE)
from st_aggrid import AgGrid, GridOptionsBuilder

@st.cache_data
def convert_df_to_csv(df:pd.DataFrame):
   return df.to_csv(index=False).encode('utf-8')

class ExpenditureGraph(TimeRangeView):
    def __init__(self, db_conn_str):
        super().__init__(db_conn_str, 1)

    def view_name(self):
        return "Expenditure Graph"
    


    def data_view(self, start_date:datetime.date, end_date:datetime.date) -> None:
        conn = psycopg2.connect(self.db_conn_str)
        cur = conn.cursor()

        # Fetch data from the database
        query = f"""
            with loan_corrections as(
                select l.debit_tx_reference, sum(l.tx_amount_borrowed) as tx_amount_borrowed
                FROM {TX_SCHEMA}.{LOAN_TABLE} l
                where l.debit_tx_reference is not null
                AND l.tx_date >= '{start_date}' AND l.tx_date <= '{end_date}'
                group by l.debit_tx_reference
                having sum(l.tx_amount_borrowed) < 0
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
                select 'debit' as source, 
                dt.tx_amount + COALESCE(-lc.tx_amount_borrowed,0) as tx_amount,
                dt.tx_category as tx_category_id,
                dt.remarks as remarks,
                dt.desc_json::text as description,
                dt.tx_date as tx_date,
                dt.id::text as id
            FROM
                {TX_SCHEMA}.{DEBIT_TX_TABLE} dt
            LEFT JOIN loan_corrections lc
                on lc.tx_amount_borrowed < 0 and dt.id = lc.debit_tx_reference
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
                atxc.tx_amount < 0
                AND atxc.category NOT IN ('Foreign Transfer') AND atxc.subcategory not in ('Rent', 'Direct Debit')
            order by atxc.tx_amount, atxc.tx_date desc
            """
        cur.execute(query=query)

        all_exp_tx_entries = cur.fetchall()
        all_exp_tx_entry_df = pd.DataFrame(
            all_exp_tx_entries, columns=[desc[0] for desc in cur.description]
        )
        
        st.info("Total Expenditure: " + str(all_exp_tx_entry_df["tx_amount"].sum()))

        all_tx_cat_df = (
            all_exp_tx_entry_df.groupby(["category", "subcategory"])["tx_amount"]
            .sum()
            .reset_index()
            .sort_values("tx_amount", ascending=False)
        )
        expenditure_graph = px.sunburst(
            all_tx_cat_df,
            path=["category", "subcategory"],
            values="tx_amount",
            color="tx_amount",
            color_continuous_scale="viridis",
        )
        expenditure_graph.update_layout(height=1500)
        st.plotly_chart(expenditure_graph, use_container_width=True)


        go = GridOptionsBuilder.from_dataframe(all_exp_tx_entry_df)
        go.configure_column("description", tooltipField="description")
        go.configure_grid_options(tooltipShowDelay=100)
        AgGrid(all_exp_tx_entry_df, 
               fit_columns_on_grid_load=True, 
               height=1500,
               gridOptions=go.build())

        all_exp_tx_csv = convert_df_to_csv(all_exp_tx_entry_df)
        st.download_button("Download Statement", all_exp_tx_csv, f"{start_date}_to_{end_date}_all-tx.csv", "text/csv", key='download-all-tx-csv')

        st.dataframe(all_tx_cat_df, use_container_width=True, height=1000)