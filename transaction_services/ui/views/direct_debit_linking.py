
import psycopg2
import pandas as pd
import streamlit as st
from .base_views import TimeRangeView
import datetime
from . import (
    TX_SCHEMA,
    DEBIT_TX_TABLE,
    CREDIT_CRD_TX_TABLE,
    TX_CATEGORY_TABLE
)

class DirectDebitLinking(TimeRangeView):
    def __init__(self, db_conn_str):
        super().__init__(db_conn_str=db_conn_str, months_of_history=3)

    def view_name(self):
        return "Direct Debit Linking"

    def data_view(self, start_date:datetime.date, end_date:datetime.date) -> None:
        conn = psycopg2.connect(self.db_conn_str)
        cur = conn.cursor()

        cur.execute(f"""
                select
                cdt.statement_id_in_file,
                cdt.statement_file_name,
                cdt.tx_amount,
                tc.category,
                tc.subcategory,
                cdt.remarks,
                cdt.recurrence,
                cdt.descriptions,
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
        credit_transaction_data = cur.fetchall()
        credit_transactions_df = pd.DataFrame(
            credit_transaction_data, columns=[desc[0] for desc in cur.description]
        )

        # Fetch data from the database
        cur.execute(f"""
                select
                dt.id,
                dt.tx_amount,
                tc.category,
                tc.subcategory,
                dt.remarks,
                dt.recurrence,
                dt.desc_json,
                dt.description,
                dt.tx_date,
                dt.start_balance,
                dt.end_balance
            from
                {TX_SCHEMA}.{DEBIT_TX_TABLE} dt
            left join {TX_SCHEMA}.{TX_CATEGORY_TABLE} tc on
                dt.tx_category = tc.id
            where
                dt.tx_date >=  '{start_date}' and dt.tx_date <='{end_date}'
                and dt.description ilike '%INT CARD SERVICES%'
            order by
                CASE WHEN tc.category IS NULL THEN 0 ELSE 1 END,
                dt.tx_date desc, dt.id desc
            """)
        abn_transaction_data = cur.fetchall()
        abn_transactions_df = pd.DataFrame(
            abn_transaction_data, columns=[desc[0] for desc in cur.description]
        )

        col_left, col_right = st.columns([0.6, 0.4])
        with col_left:
            selected_abn_transactions = st.dataframe(
                abn_transactions_df,
                on_select="rerun",
                use_container_width=True,
                selection_mode="single-row",
                column_config={"_index": None},
                key="abn_transactions",
                height=1200,
            )

        with col_right:
            selected_credit_transactions = st.dataframe(
                credit_transactions_df,
                on_select="rerun",
                use_container_width=True,
                selection_mode="single-row",
                column_config={"_index": None},
                key="existing_tx_categories",
                height=1200,
            )

        selected_abn_order_rows = []
        if selected_abn_transactions is not None:
            selected_abn_order_rows = [
                    {"id":int(row["id"]), "tx_amount":row["tx_amount"]}
                    for row in abn_transactions_df.iloc[
                        selected_abn_transactions["selection"]["rows"]
                    ].to_dict(orient="index").values()
                ]
            with col_left:
                st.write(selected_abn_order_rows)
            
        selected_credit_tx_record = []
        if selected_credit_transactions is not None:
            selected_credit_tx_record = [
                {"p_key":(row["statement_file_name"], int(row["statement_id_in_file"])), "tx_amount":row["tx_amount"]}
                for row in credit_transactions_df.iloc[
                selected_credit_transactions["selection"]["rows"]].to_dict(orient="index").values()
            ]
            with col_right:
                st.write(selected_credit_tx_record)
        if st.button("Link Cash Direct Debit"):
                if selected_abn_order_rows[0]["tx_amount"]+selected_credit_tx_record[0]["tx_amount"] != 0:
                    st.error("Amounts don't match")
                    return
                cur.execute(
                    f"""
                            UPDATE {TX_SCHEMA}.{CREDIT_CRD_TX_TABLE}
                            set direct_debit_link = %s
                            where (statement_file_name, statement_id_in_file) = %s
                            """,
                    (int(selected_abn_order_rows[0]["id"]), tuple(selected_credit_tx_record[0]["p_key"])),
                )
                conn.commit()

        cur.close()
        conn.close()
