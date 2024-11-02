import psycopg2
import pandas as pd
import streamlit as st
from .base_views import TimeRangeView 
import datetime
from . import (
    TX_SCHEMA,
    LOAN_TABLE,
)


class ManageLoanEntries(TimeRangeView):
    def __init__(self, db_conn_str):
        super().__init__(db_conn_str=db_conn_str, months_of_history=3)

    def view_name(self):
        return "Manage Loan Entries"


    def data_view(self, start_date:datetime.date, end_date:datetime.date) -> None:
        conn = psycopg2.connect(self.db_conn_str)
        cur = conn.cursor()

        # Fetch data from the database
        cur.execute(
            f"""SELECT id, tx_amount_borrowed, counterparty, remarks, tx_date, currency, foreign_amt_borrowed, settling_loan_tx_link
            FROM {TX_SCHEMA}.{LOAN_TABLE}
            WHERE tx_date >= '{start_date}' and tx_date <= '{end_date}'
            order by id desc"""
        )  # Replace with your actual table name
        data = cur.fetchall()
        existing_loans = pd.DataFrame(
            data, columns=[desc[0] for desc in cur.description]
        )

        # Display Database
        existing_row_selection = st.dataframe(
            existing_loans,
            use_container_width=True,
            on_select="rerun",
            key="existing_loans",
            selection_mode="multi-row",
            column_config={"_index": None},
        )
        loan_ids_to_settle = None
        if existing_row_selection is not None:
            loan_ids_to_settle = existing_loans.iloc[
                existing_row_selection["selection"]["rows"]
            ]["id"].to_dict().values()
        st.write(loan_ids_to_settle)

        if st.button("Create Settlement Entry"):
            if loan_ids_to_settle: 
                loan_amt_to_settle =  existing_loans.iloc[
                    existing_row_selection["selection"]["rows"]
                ]["tx_amount_borrowed"].sum()

                counterparties = ",".join(set(existing_loans.iloc[
                    existing_row_selection["selection"]["rows"]
                ]["counterparty"].to_list()))
                
                currency = set(existing_loans.iloc[
                    existing_row_selection["selection"]["rows"]
                ]["currency"].to_list())
                
                if len(currency) > 1:
                    st.error(f"Mixed currencies {currency}")
                    return

                cur.execute(
                    f"""INSERT INTO {TX_SCHEMA}.{LOAN_TABLE} (tx_amount_borrowed, counterparty, remarks, currency, tx_date, foreign_amt_borrowed) 
                    VALUES (%s, %s, %s, %s, %s, %s) 
                    RETURNING id""",
                    (-loan_amt_to_settle, counterparties, "Loan settlement", currency.pop(), datetime.date.today(), None)
                )
                inserted_settlement_record_id = cur.fetchone()
                cur.execute(
                    f"""UPDATE {TX_SCHEMA}.{LOAN_TABLE}
                    SET settling_loan_tx_link = %s
                    WHERE id IN %s""",
                    (inserted_settlement_record_id, tuple(loan_ids_to_settle))
                )
                conn.commit()


        # Delete selected rows
        if st.button("Delete Selected Rows"):
            if existing_row_selection is not None:
                loan_ids_to_settle = existing_loans.iloc[
                    existing_row_selection["selection"]["rows"]
                ]["id"].to_list()
                cur.execute(
                    f"DELETE FROM {TX_SCHEMA}.{LOAN_TABLE} WHERE id IN %s",
                    (tuple(loan_ids_to_settle),),
                )
                conn.commit()

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
                "tx_amount_borrowed": st.column_config.NumberColumn(required=True),
                "foreign_amt_borrowed": st.column_config.NumberColumn(),
                "currency": st.column_config.TextColumn(default="EUR"),
                "tx_date": st.column_config.DateColumn(default=datetime.date.today())
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
            )
            for row in new_rows_df.dropna(
                subset=["tx_amount_borrowed", "counterparty", "remarks", "tx_date"]
            ).to_dict(orient="records")
        ]

        # Add new row
        if st.button("Add Rows"):
            if len(data_to_insert) > 0:
                try:
                    cur.executemany(
                        f"INSERT INTO {TX_SCHEMA}.{LOAN_TABLE} (tx_amount_borrowed, counterparty, remarks, currency, tx_date, foreign_amt_borrowed) VALUES (%s, %s, %s, %s, %s, %s)",
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
