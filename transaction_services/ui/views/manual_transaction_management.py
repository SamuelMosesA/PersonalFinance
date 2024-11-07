import psycopg2
import pandas as pd
import streamlit as st
from .base_views import TimeRangeView
import datetime
import json

from transaction_services.config.db_constants import (
    TX_SCHEMA,
    MANUAL_TX_TABLE,
    DEBIT_TX_TABLE,
    TX_CATEGORY_TABLE,
)


class ManageManualTxEntries(TimeRangeView):
    def __init__(self, db_conn_str):
        super().__init__(db_conn_str=db_conn_str, months_of_history=3)

    def view_name(self):
        return "Manage Manual Tx Entries"

    def data_view(self, start_date: datetime.date, end_date: datetime.date) -> None:
        conn = psycopg2.connect(self.db_conn_str)
        cur = conn.cursor()

        # Fetch data from the database
        cur.execute(
            f"""SELECT id, tx_amount, currency, description, tx_date, remarks 
            FROM {TX_SCHEMA}.{MANUAL_TX_TABLE} 
            WHERE tx_date >= '{start_date}' and tx_date <= '{end_date}'
            order by id desc"""
        )  # Replace with your actual table name
        data = cur.fetchall()
        existing_manual_tx = pd.DataFrame(
            data, columns=[desc[0] for desc in cur.description]
        )

        # Display Database
        existing_row_selection = st.dataframe(
            existing_manual_tx,
            use_container_width=True,
            on_select="rerun",
            key="existing_loans",
            selection_mode="multi-row",
            column_config={"_index": None},
        )

        # Delete selected rows
        if st.button("Delete Selected Rows"):
            if existing_row_selection is not None:
                manual_tx_ids_to_delete = existing_manual_tx.iloc[
                    existing_row_selection["selection"]["rows"]
                ]["id"].to_list()
                cur.execute(
                    f"DELETE FROM {TX_SCHEMA}.{MANUAL_TX_TABLE} WHERE id IN %s",
                    (tuple(manual_tx_ids_to_delete),),
                )
                conn.commit()

        # Create a separate DataFrame for adding new rows
        new_rows_df = pd.DataFrame(
            columns=[
                "tx_amount",
                "tx_date",
                "remarks",
                "currency",
                "description",
            ]
        )
        new_rows_df = st.data_editor(
            new_rows_df,
            num_rows="dynamic",
            use_container_width=True,
            key="new_loan_rows",
            column_config={
                "tx_amount": st.column_config.NumberColumn(required=True),
                "tx_date": st.column_config.DateColumn(required=True),
                "currency": st.column_config.TextColumn(default="EUR"),
                "remarks": st.column_config.TextColumn(required=True),
                "description": st.column_config.TextColumn(required=True),
            },
        )

        data_to_insert = [
            (
                row["tx_amount"],
                row["tx_date"],
                row["description"],
                row["remarks"],
                row["currency"],
            )
            for row in new_rows_df.dropna(
                subset=["tx_amount", "tx_date", "currency", "remarks", "description"]
            ).to_dict(orient="records")
        ]

        # Add new row
        if st.button("Add Rows"):
            if len(data_to_insert) > 0:
                try:
                    cur.executemany(
                        f"INSERT INTO {TX_SCHEMA}.{MANUAL_TX_TABLE} (tx_amount, tx_date, description, remarks, currency) VALUES (%s, %s, %s, %s, %s)",
                        data_to_insert,
                    )
                    conn.commit()
                except Exception as e:
                    st.error(e)
            else:
                st.warning("Please add rows before clicking 'Add Row'.")

        st.write(data_to_insert)
        # Close the connection
        cur.close()
        conn.close()


class CorrectDebitTx(TimeRangeView):
    def __init__(self, db_conn_str):
        super().__init__(db_conn_str=db_conn_str, months_of_history=3)

    def view_name(self):
        return "Add ABN Correction Entries"

    def data_view(self, start_date: datetime.date, end_date: datetime.date) -> None:
        conn = psycopg2.connect(self.db_conn_str)
        cur = conn.cursor()

        # Fetch data from the database
        cur.execute(
            f"""SELECT id, tx_amount, currency, description, tx_date, remarks 
            FROM {TX_SCHEMA}.{MANUAL_TX_TABLE} 
            WHERE correcting_tx_date >= '{start_date}' and correcting_tx_date <= '{end_date}'
            order by id desc"""
        )  # Replace with your actual table name
        data = cur.fetchall()
        existing_manual_tx = pd.DataFrame(
            data, columns=[desc[0] for desc in cur.description]
        )

        # Display Database
        existing_manual_tx_row_selection = st.dataframe(
            existing_manual_tx,
            use_container_width=True,
            on_select="rerun",
            key="existing_loans",
            selection_mode="multi-row",
            column_config={"_index": None},
        )

        # Delete selected rows
        if st.button("Delete Selected Rows"):
            if existing_manual_tx_row_selection is not None:
                manual_tx_ids_to_delete = existing_manual_tx.iloc[
                    existing_manual_tx_row_selection["selection"]["rows"]
                ]["id"].to_list()
                cur.execute(
                    f"DELETE FROM {TX_SCHEMA}.{MANUAL_TX_TABLE} WHERE id IN %s",
                    (tuple(manual_tx_ids_to_delete),),
                )
                conn.commit()

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
                dt.tx_date >= '{start_date}' and dt.tx_date <='{end_date}'
            order by
                CASE WHEN tc.category IS NULL THEN 0 ELSE 1 END,
                dt.tx_date desc, dt.id desc
            """)
        abn_transaction_data = cur.fetchall()
        abn_transactions_df = pd.DataFrame(
            abn_transaction_data, columns=[desc[0] for desc in cur.description]
        )

        selected_abn_transaction = st.dataframe(
            abn_transactions_df,
            on_select="rerun",
            use_container_width=True,
            selection_mode="single-row",
            column_config={"_index": None},
            key="abn_transactions",
            height=1200,
        )

        if (
            selected_abn_transaction is not None
            and len(selected_abn_transaction["selection"]["rows"]) > 0
        ):
            selected_abn_order_row = [
                row
                for row in abn_transactions_df.iloc[
                    selected_abn_transaction["selection"]["rows"]
                ]
                .to_dict(orient="index")
                .values()
            ][0]
            st.write(selected_abn_order_row)

            # Create a separate DataFrame for adding new rows
            new_rows_df = pd.DataFrame(
                columns=[
                    "tx_amount",
                    "tx_date",
                    "remarks",
                    "currency",
                    "description",
                ]
            )
            new_rows_df = st.data_editor(
                new_rows_df,
                num_rows="dynamic",
                use_container_width=True,
                key="new_loan_rows",
                column_config={
                    "tx_amount": st.column_config.NumberColumn(
                        required=True,
                        default=float(selected_abn_order_row["tx_amount"]),
                    ),
                    "tx_date": st.column_config.DateColumn(
                        required=True, default=selected_abn_order_row["tx_date"]
                    ),
                    "currency": st.column_config.TextColumn(default="EUR"),
                    "remarks": st.column_config.TextColumn(
                        required=True, default=selected_abn_order_row["remarks"]
                    ),
                    "description": st.column_config.TextColumn(
                        required=True,
                        default=json.dumps(selected_abn_order_row["desc_json"]),
                    ),
                },
            )

            data_to_insert = [
                (
                    row["tx_amount"],
                    row["tx_date"],
                    row["description"],
                    row["remarks"],
                    row["currency"],
                    selected_abn_order_row["id"],
                    selected_abn_order_row["tx_date"],
                )
                for row in new_rows_df.dropna(
                    subset=[
                        "tx_amount",
                        "tx_date",
                        "currency",
                        "remarks",
                        "description",
                    ]
                ).to_dict(orient="records")
            ]

            # Add new row
            if st.button("Add Corrections"):
                if len(data_to_insert) > 0:
                    try:
                        cur.executemany(
                            f"INSERT INTO {TX_SCHEMA}.{MANUAL_TX_TABLE} (tx_amount, tx_date, description, remarks, currency, correcting_debit_tx_ref, correcting_tx_date) VALUES (%s, %s, %s, %s, %s, %s, %s)",
                            data_to_insert,
                        )
                        conn.commit()
                    except Exception as e:
                        st.error(e)
                else:
                    st.warning("Please add rows before clicking 'Add Row'.")

            st.write(data_to_insert)
        # Close the connection
        cur.close()
        conn.close()
