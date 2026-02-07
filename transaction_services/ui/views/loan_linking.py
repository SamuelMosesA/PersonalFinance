import psycopg2
import pandas as pd
import streamlit as st
from transaction_services.ui.views.base_views import TimeRangeView
import datetime
from transaction_services.config.db_constants import (
    TX_SCHEMA,
    TX_CATEGORY_TABLE,
    DEBIT_TX_TABLE,
    MANUAL_TX_TABLE,
    CREDIT_CRD_TX_TABLE,
    LOAN_TABLE,
)


class DebitTxLoanLinking(TimeRangeView):
    def __init__(self, db_conn_str):
        super().__init__(db_conn_str=db_conn_str, months_of_history=3)

    def view_name(self):
        return "Loan link ABN transactions"

    def data_view(self, start_date: datetime.date, end_date: datetime.date) -> None:
        conn = psycopg2.connect(self.db_conn_str)
        cur = conn.cursor()

        # Fetch data from the database
        cur.execute(
            f"""SELECT id, tx_amount_borrowed, is_settlement, counterparty, remarks, tx_date, debit_tx_reference, currency, foreign_amt_borrowed
            FROM {TX_SCHEMA}.{LOAN_TABLE}
            WHERE tx_date >= '{start_date}' and tx_date <= '{end_date}'
            order by id desc"""
        )  # Replace with your actual table name
        data = cur.fetchall()
        existing_loans_df = pd.DataFrame(
            data, columns=[desc[0] for desc in cur.description]
        )

        # Fetch data from the database
        cur.execute(f"""
                select
                dt.id,
                dt.bank,
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

        col_left, col_right = st.columns([0.5, 0.5])
        with col_left:
            selected_abn_transaction = st.dataframe(
                abn_transactions_df,
                on_select="rerun",
                use_container_width=True,
                selection_mode="single-row",
                column_config={"_index": None},
                key="abn_transactions",
                height=1200,
            )

        with col_right:
            selected_loans = st.dataframe(
                existing_loans_df,
                on_select="rerun",
                use_container_width=True,
                selection_mode="multi-row",
                column_config={"_index": None},
                key="existing_tx_categories",
                height=1200,
            )

        selected_abn_tx_data = None
        if selected_abn_transaction is not None:
            selected_abn_tx_data = abn_transactions_df.iloc[
                selected_abn_transaction["selection"]["rows"]
            ].to_dict(orient="records")

        if selected_loans is not None:
            selected_loan_data = existing_loans_df.iloc[
                selected_loans["selection"]["rows"]
            ].to_dict(orient="records")

        if st.button("Link Loan"):
            if len(selected_abn_tx_data) > 0 and len(selected_loan_data) > 0:
                selected_loan_ids = [
                    loan_record["id"] for loan_record in selected_loan_data
                ]
                selected_loan_tx_dates = set(
                    [loan_record["tx_date"] for loan_record in selected_loan_data]
                )

            if len(selected_loan_tx_dates) > 1:
                st.error(f"More that one loan tx date: {selected_loan_tx_dates}")
                return
            selected_loan_tx_date = selected_loan_tx_dates.pop()
            selected_abn_tx_date = selected_abn_tx_data[0]["tx_date"]
            if selected_loan_tx_date != selected_abn_tx_date:
                st.error(
                    f"Loan tx date {selected_loan_tx_date} != debit tx date {selected_abn_tx_date}"
                )
                return

            cur.execute(
                f"""
                        UPDATE {TX_SCHEMA}.{LOAN_TABLE}
                        set debit_tx_reference = %s
                        where id in %s
                        """,
                (int(selected_abn_tx_data[0]["id"]), tuple(selected_loan_ids)),
            )
            conn.commit()

        # Fetch data from the database
        # Close the connection
        cur.close()
        conn.close()

class DebitLoanManageAndLink(TimeRangeView):
    def __init__(self, db_conn_str):
        super().__init__(db_conn_str=db_conn_str, months_of_history=3)

    def view_name(self):
        return "Debit Loan Manage And Link"

    def data_view(self, start_date: datetime.date, end_date: datetime.date) -> None:
        conn = psycopg2.connect(self.db_conn_str)
        cur = conn.cursor()

        # Fetch data from the database
        cur.execute(
            f"""SELECT id, tx_amount_borrowed, is_settlement, counterparty, remarks, tx_date, debit_tx_reference, currency, foreign_amt_borrowed
            FROM {TX_SCHEMA}.{LOAN_TABLE}
            WHERE tx_date >= '{start_date}' and tx_date <= '{end_date}'
            order by id desc"""
        )  # Replace with your actual table name
        data = cur.fetchall()
        existing_loan_tx = pd.DataFrame(
            data, columns=[desc[0] for desc in cur.description]
        )

        # Display Database
        existing_loan_row_selection = st.dataframe(
            existing_loan_tx,
            use_container_width=True,
            on_select="rerun",
            key="existing_loans",
            selection_mode="multi-row",
            column_config={"_index": None},
        )

        # Delete selected rows
        if st.button("Delete Selected Rows"):
            if existing_loan_row_selection is not None:
                manual_loan_ids_to_delete = existing_loan_tx.iloc[
                    existing_loan_row_selection["selection"]["rows"]
                ]["id"].to_list()
                cur.execute(
                    f"DELETE FROM {TX_SCHEMA}.{MANUAL_TX_TABLE} WHERE id IN %s",
                    (tuple(manual_loan_ids_to_delete),),
                )
                conn.commit()

        cur.execute(f"""
                select
                dt.id,
                dt.bank,
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
                    "tx_amount_borrowed",
                    "counterparty",
                    "remarks",
                    "tx_date",
                    "currency",
                    "foreign_amt_borrowed",
                ]
            )
            new_rows_df = st.data_editor(
                new_rows_df,
                num_rows="dynamic",
                use_container_width=True,
                key="new_loan_rows",
                column_config={
                    "tx_amount_borrowed": st.column_config.NumberColumn(
                        required=True,
                        default=float(selected_abn_order_row["tx_amount"]),
                        ),
                    "foreign_amt_borrowed": st.column_config.NumberColumn(),
                    "currency": st.column_config.TextColumn(required=True, default="EUR"),
                    "tx_date": st.column_config.DateColumn(
                        required=True, default=selected_abn_order_row["tx_date"]),
                },
            )

            data_to_insert = [
                (
                    row["tx_amount_borrowed"],
                    row["counterparty"],
                    row["remarks"],
                    row["currency"],
                    row["tx_date"],
                    row["foreign_amt_borrowed"],
                    selected_abn_order_row["id"],
                )
                for row in new_rows_df.dropna(
                    subset=["tx_amount_borrowed", "counterparty", "remarks", "tx_date"]
                ).to_dict(orient="records")
            ]

            # Add new row
            if st.button("Add Loans"):
                if len(data_to_insert) > 0:
                    try:
                        cur.executemany(
                            f"INSERT INTO {TX_SCHEMA}.{LOAN_TABLE} (tx_amount_borrowed, counterparty, remarks, currency, tx_date, foreign_amt_borrowed, debit_tx_reference) VALUES (%s, %s, %s, %s, %s, %s, %s)",
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


class ManualTxLoanLinking(TimeRangeView):
    def __init__(self, db_conn_str):
        super().__init__(db_conn_str=db_conn_str, months_of_history=3)

    def view_name(self):
        return "Loan link Manual Transactions"

    def data_view(self, start_date: datetime.date, end_date: datetime.date) -> None:
        conn = psycopg2.connect(self.db_conn_str)
        cur = conn.cursor()

        # Fetch data from the database
        cur.execute(
            f"""SELECT id, tx_amount_borrowed, is_settlement, counterparty, remarks, tx_date, currency, foreign_amt_borrowed
            FROM {TX_SCHEMA}.{LOAN_TABLE}
            WHERE tx_date >= '{start_date}' and tx_date <= '{end_date}'
            order by id desc"""
        )  # Replace with your actual table name
        data = cur.fetchall()
        existing_loans_df = pd.DataFrame(
            data, columns=[desc[0] for desc in cur.description]
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
                dt.description,
                dt.tx_date
            from
                {TX_SCHEMA}.{MANUAL_TX_TABLE} dt
            left join {TX_SCHEMA}.{TX_CATEGORY_TABLE} tc on
                dt.tx_category = tc.id
            where
                dt.tx_date >= '{start_date}' and dt.tx_date <='{end_date}'
            order by
                CASE WHEN tc.category IS NULL THEN 0 ELSE 1 END,
                dt.tx_date desc, dt.id desc
            """)
        manual_transaction_data = cur.fetchall()
        manual_transactions_df = pd.DataFrame(
            manual_transaction_data, columns=[desc[0] for desc in cur.description]
        )

        col_left, col_right = st.columns([0.5, 0.5])
        with col_left:
            selected_manual_transaction = st.dataframe(
                manual_transactions_df,
                on_select="rerun",
                use_container_width=True,
                selection_mode="single-row",
                column_config={"_index": None},
                key="abn_transactions",
                height=1200,
            )

        with col_right:
            selected_loans = st.dataframe(
                existing_loans_df,
                on_select="rerun",
                use_container_width=True,
                selection_mode="multi-row",
                column_config={"_index": None},
                key="existing_tx_categories",
                height=1200,
            )

        selected_manual_tx_data = None
        if selected_manual_transaction is not None:
            selected_manual_tx_data = manual_transactions_df.iloc[
                selected_manual_transaction["selection"]["rows"]
            ].to_dict(orient="records")

        if selected_loans is not None:
            selected_loan_data = existing_loans_df.iloc[
                selected_loans["selection"]["rows"]
            ].to_dict(orient="records")

        if st.button("Link Loan"):
            if len(selected_manual_tx_data) > 0 and len(selected_loan_data) > 0:
                selected_loan_ids = [
                    loan_record["id"] for loan_record in selected_loan_data
                ]
                selected_loan_tx_dates = set(
                    [loan_record["tx_date"] for loan_record in selected_loan_data]
                )

            if len(selected_loan_tx_dates) > 1:
                st.error(f"More that one loan tx date: {selected_loan_tx_dates}")
                return
            selected_loan_tx_date = selected_loan_tx_dates.pop()
            selected_manual_tx_date = selected_manual_tx_data[0]["tx_date"]
            if selected_loan_tx_date != selected_manual_tx_date:
                st.error(
                    f"Loan tx date {selected_loan_tx_date} != manual tx date {selected_manual_tx_date}"
                )
                return

            cur.execute(
                f"""
                        UPDATE {TX_SCHEMA}.{LOAN_TABLE}
                        set manual_tx_reference = %s
                        where id in %s
                        """,
                (int(selected_manual_tx_data[0]["id"]), tuple(selected_loan_ids)),
            )
            conn.commit()


class CreditCrdLoanLinking(TimeRangeView):
    def __init__(self, db_conn_str):
        super().__init__(db_conn_str=db_conn_str, months_of_history=3)

    def view_name(self):
        return "Loan link Credit Card transactions"

    def data_view(self, start_date: datetime.date, end_date: datetime.date) -> None:
        conn = psycopg2.connect(self.db_conn_str)
        cur = conn.cursor()

        cur.execute(
            f"""SELECT id, tx_amount_borrowed, is_settlement, counterparty, remarks, tx_date, currency, foreign_amt_borrowed
            FROM {TX_SCHEMA}.{LOAN_TABLE}
            WHERE tx_date >= '{start_date}' and tx_date <= '{end_date}'
            order by id desc"""
        )  # Replace with your actual table name
        data = cur.fetchall()
        existing_loans_df = pd.DataFrame(
            data, columns=[desc[0] for desc in cur.description]
        )

        # Fetch data from the database
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
                cdt.tx_date >= '{start_date}' and cdt.tx_date <='{end_date}'
                AND cdt.direct_debit_link is null
            order by
                CASE WHEN tc.category IS NULL THEN 0 ELSE 1 END,
                cdt.tx_date desc, cdt.statement_file_name, cdt.statement_id_in_file desc
            """)
        credit_transaction_data = cur.fetchall()
        credit_transactions_df = pd.DataFrame(
            credit_transaction_data, columns=[desc[0] for desc in cur.description]
        )

        col_left, col_right = st.columns([0.5, 0.5])
        with col_left:
            selected_credit_transaction = st.dataframe(
                credit_transactions_df,
                on_select="rerun",
                use_container_width=True,
                selection_mode="single-row",
                column_config={"_index": None},
                key="abn_transactions",
                height=1200,
            )

        with col_right:
            selected_loans = st.dataframe(
                existing_loans_df,
                on_select="rerun",
                use_container_width=True,
                selection_mode="multi-row",
                column_config={"_index": None},
                key="existing_tx_categories",
                height=1200,
            )

        selected_credit_tx_data = None
        if selected_credit_tx_data is not None:
            selected_credit_tx_data = credit_transactions_df.iloc[
                selected_credit_transaction["selection"]["rows"]
            ].to_dict(orient="records")

        if selected_loans is not None:
            selected_loan_data = existing_loans_df.iloc[
                selected_loans["selection"]["rows"]
            ].to_dict(orient="records")

        if st.button("Link Loan"):
            if len(selected_credit_tx_data) > 0 and len(selected_loan_data) > 0:
                selected_loan_ids = [
                    loan_record["id"] for loan_record in selected_loan_data
                ]
                selected_loan_tx_dates = set(
                    [loan_record["tx_date"] for loan_record in selected_loan_data]
                )

            if len(selected_loan_tx_dates) > 1:
                st.error(f"More that one loan tx date: {selected_loan_tx_dates}")
                return
            selected_loan_tx_date = selected_loan_tx_dates.pop()
            selected_credit_tx_date = selected_credit_tx_data[0]["tx_date"]
            if selected_loan_tx_date != selected_credit_tx_date:
                st.error(
                    f"Loan tx date {selected_loan_tx_date} != credit tx date {selected_credit_tx_date}"
                )
                return

            selected_credit_tx = selected_credit_tx_data[0]
            cur.execute(
                f"""
                        UPDATE {TX_SCHEMA}.{LOAN_TABLE}
                        set (credit_tx_stmt_file_ref, credit_tx_stmt_id_ref) = %s
                        where id in %s
                        """,
                (
                    (
                        selected_credit_tx["statement_id_in_file"],
                        selected_credit_tx["statement_file_name"],
                    ),
                    tuple(selected_loan_ids),
                ),
            )
            conn.commit()
