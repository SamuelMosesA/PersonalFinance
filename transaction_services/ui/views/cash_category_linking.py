import psycopg2
import pandas as pd
import streamlit as st
from .base_views import BaseStreamlitView, TimeRangeView
from typing import Optional
import datetime
from transaction_services.config.db_constants import (
    TX_SCHEMA,
    TX_CATEGORY_TABLE,
    DEBIT_TX_TABLE,
    MANUAL_TX_TABLE,
    CREDIT_CRD_TX_TABLE,
)


class ManageCashCategories(BaseStreamlitView):
    def __init__(self, db_conn_str):
        super().__init__(db_conn_str)

    def view_name(self):
        return "Manage Tx Categories"

    @st.fragment
    def view_fragment(self) -> None:
        if st.button("Refresh"):
            st.rerun(scope="fragment")
        conn = psycopg2.connect(self.db_conn_str)
        cur = conn.cursor()

        # Fetch data from the database
        cur.execute(
            f"SELECT id, category, subcategory FROM {TX_SCHEMA}.{TX_CATEGORY_TABLE} order by category, subcategory"
        )  # Replace with your actual table name
        data = cur.fetchall()
        existing_tx_categories = pd.DataFrame(
            data, columns=[desc[0] for desc in cur.description]
        )

        # Display Database
        existing_row_selection = st.dataframe(
            existing_tx_categories,
            use_container_width=True,
            on_select="rerun",
            key="existing_tx_cat_data",
            selection_mode="multi-row",
            column_config={"_index": None},
        )

        # Delete selected rows
        if st.button("Delete Selected Rows"):
            if existing_row_selection is not None:
                cat_ids_to_delete = existing_tx_categories.iloc[
                    existing_row_selection["selection"]["rows"]
                ]["id"].to_list()
                cur.execute(
                    f"DELETE FROM {TX_SCHEMA}.{TX_CATEGORY_TABLE} WHERE id IN %s",
                    (tuple(cat_ids_to_delete),),
                )
                conn.commit()

        # Create a separate DataFrame for adding new rows
        new_rows_df = pd.DataFrame(columns=["category", "subcategory"])
        new_rows_df = st.data_editor(
            new_rows_df,
            num_rows="dynamic",
            use_container_width=True,
            key="new_tx_cat_rows",
        )
        data_to_insert = [
            (row["category"], row["subcategory"])
            for row in new_rows_df.dropna().to_dict(orient="records")
        ]

        # Add new row
        if st.button("Add Rows"):
            if len(data_to_insert) > 0:
                try:
                    cur.executemany(
                        f"INSERT INTO {TX_SCHEMA}.{TX_CATEGORY_TABLE} (category, subcategory) VALUES (%s, %s)",
                        data_to_insert,
                    )
                    conn.commit()
                except Exception as e:
                    st.error(e)
            else:
                st.warning(
                    "Please add a row with category and subcategory before clicking 'Add Row'."
                )

        st.write(data_to_insert)
        # Close the connection
        cur.close()
        conn.close()

    def render(self):
        st.header("Manage Tx Categories")
        self.view_fragment()


class DebitCashCategoryLinking(TimeRangeView):
    def __init__(self, db_conn_str):
        super().__init__(db_conn_str=db_conn_str, months_of_history=3)

    def view_name(self):
        return "Cash Category link ABN transactions"

    def data_view(self, start_date: datetime.date, end_date: datetime.date) -> None:
        conn = psycopg2.connect(self.db_conn_str)
        cur = conn.cursor()

        cur.execute(
            f"SELECT id, category, subcategory FROM {TX_SCHEMA}.{TX_CATEGORY_TABLE} order by category, subcategory"
        )
        tx_category_data = cur.fetchall()
        existing_tx_categories_df = pd.DataFrame(
            tx_category_data, columns=["id", "category", "subcategory"]
        )

        # Fetch data from the database
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
                dt.tx_date,
                dt.start_balance,
                dt.end_balance
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
        abn_transaction_data = cur.fetchall()
        abn_transactions_df = pd.DataFrame(
            abn_transaction_data, columns=[desc[0] for desc in cur.description]
        )

        def _color_unlabelled_tx(value: Optional[str]):
            if value is None:
                return "background-color: indigo"

        styled_abn_transactions = abn_transactions_df.style.map(
            _color_unlabelled_tx, subset=["category"]
        )

        col_left, col_right = st.columns([0.8, 0.2])
        with col_left:
            selected_abn_transactions = st.dataframe(
                styled_abn_transactions,
                on_select="rerun",
                use_container_width=True,
                selection_mode="multi-row",
                column_config={"_index": None},
                key="abn_transactions",
                height=1200,
            )

        with col_right:
            selected_tx_category = st.dataframe(
                existing_tx_categories_df,
                on_select="rerun",
                use_container_width=True,
                selection_mode="single-row",
                column_config={"_index": None},
                key="existing_tx_categories",
                height=1200,
            )

        selected_abn_order_numbers = []
        if selected_abn_transactions is not None:
            selected_abn_order_numbers = tuple(
                [
                    int(i)
                    for i in abn_transactions_df.iloc[
                        selected_abn_transactions["selection"]["rows"]
                    ]["id"].to_list()
                ]
            )
        if st.button("Link Cash Category"):
            selected_tx_category_id = None
            if (
                selected_tx_category is not None
                and len(selected_tx_category["selection"]["rows"]) > 0
            ):
                selected_tx_category_id = existing_tx_categories_df.iloc[
                    selected_tx_category["selection"]["rows"][0]
                ]["id"]
                st.write(selected_abn_order_numbers)
                cur.execute(
                    f"""
                            UPDATE {TX_SCHEMA}.{DEBIT_TX_TABLE}
                            set tx_category = %s
                            where id in %s
                            """,
                    (int(selected_tx_category_id), selected_abn_order_numbers),
                )
                conn.commit()

        edit_col_1, edit_col_2 = st.columns(2, gap="large")
        with edit_col_1:
            remarks_content = st.text_input("Remarks", value=None)
            if st.button("Set Remarks"):
                if len(selected_abn_order_numbers) > 0:
                    st.write(selected_abn_order_numbers)
                    cur.execute(
                        f"""
                                UPDATE {TX_SCHEMA}.{DEBIT_TX_TABLE}
                                set remarks = %s
                                where id in %s
                                """,
                        (remarks_content, selected_abn_order_numbers),
                    )
                    conn.commit()

        with edit_col_2:
            recurrence = st.selectbox(
                "Recurrence Hz", options=[None, "Monthly", "Yearly"]
            )
            if st.button("Set Recurrence Hz"):
                if len(selected_abn_order_numbers) > 0:
                    st.write(selected_abn_order_numbers)
                    cur.execute(
                        f"""
                                UPDATE {TX_SCHEMA}.{DEBIT_TX_TABLE}
                                set recurrence = %s
                                where id in %s
                                """,
                        (recurrence, selected_abn_order_numbers),
                    )
                    conn.commit()

        # Fetch data from the database
        # Close the connection
        cur.close()
        conn.close()


class ManualTxCashCategoryLinking(TimeRangeView):
    def __init__(self, db_conn_str):
        super().__init__(db_conn_str=db_conn_str, months_of_history=3)

    def view_name(self):
        return "Cash Category link Manual transactions"

    def data_view(self, start_date: datetime.date, end_date: datetime.date) -> None:
        conn = psycopg2.connect(self.db_conn_str)
        cur = conn.cursor()

        cur.execute(
            f"SELECT id, category, subcategory FROM {TX_SCHEMA}.{TX_CATEGORY_TABLE} order by category, subcategory"
        )
        tx_category_data = cur.fetchall()
        existing_tx_categories_df = pd.DataFrame(
            tx_category_data, columns=["id", "category", "subcategory"]
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
                dt.tx_date >= '{start_date}' and dt.tx_date <= '{end_date}'
            order by
                CASE WHEN tc.category IS NULL THEN 0 ELSE 1 END,
                dt.tx_date desc, dt.id desc
            """)
        abn_transaction_data = cur.fetchall()
        abn_transactions_df = pd.DataFrame(
            abn_transaction_data, columns=[desc[0] for desc in cur.description]
        )

        def _color_unlabelled_tx(value: Optional[str]):
            if value is None:
                return "background-color: indigo"

        styled_abn_transactions = abn_transactions_df.style.map(
            _color_unlabelled_tx, subset=["category"]
        )

        col_left, col_right = st.columns([0.8, 0.2])
        with col_left:
            selected_manual_transactions = st.dataframe(
                styled_abn_transactions,
                on_select="rerun",
                use_container_width=True,
                selection_mode="multi-row",
                column_config={"_index": None},
                key="abn_transactions",
                height=1200,
            )

        with col_right:
            selected_tx_category = st.dataframe(
                existing_tx_categories_df,
                on_select="rerun",
                use_container_width=True,
                selection_mode="single-row",
                column_config={"_index": None},
                key="existing_tx_categories",
                height=1200,
            )

        selected_manual_tx_ids = []
        if selected_manual_transactions is not None:
            selected_manual_tx_ids = tuple(
                [
                    int(i)
                    for i in abn_transactions_df.iloc[
                        selected_manual_transactions["selection"]["rows"]
                    ]["id"].to_list()
                ]
            )
        if st.button("Link Cash Category"):
            selected_tx_category_id = None
            if (
                selected_tx_category is not None
                and len(selected_tx_category["selection"]["rows"]) > 0
            ):
                selected_tx_category_id = existing_tx_categories_df.iloc[
                    selected_tx_category["selection"]["rows"][0]
                ]["id"]
                st.write(selected_manual_tx_ids)
                cur.execute(
                    f"""
                            UPDATE {TX_SCHEMA}.{MANUAL_TX_TABLE}
                            set tx_category = %s
                            where id in %s
                            """,
                    (int(selected_tx_category_id), selected_manual_tx_ids),
                )
                conn.commit()

        edit_col_1, edit_col_2 = st.columns(2, gap="large")
        with edit_col_1:
            remarks_content = st.text_input("Remarks", value=None)
            if st.button("Set Remarks"):
                if len(selected_manual_tx_ids) > 0:
                    st.write(selected_manual_tx_ids)
                    cur.execute(
                        f"""
                                UPDATE {TX_SCHEMA}.{MANUAL_TX_TABLE}
                                set remarks = %s
                                where id in %s
                                """,
                        (remarks_content, selected_manual_tx_ids),
                    )
                    conn.commit()

        with edit_col_2:
            recurrence = st.selectbox(
                "Recurrence Hz", options=[None, "Monthly", "Yearly"]
            )
            if st.button("Set Recurrence Hz"):
                if len(selected_manual_tx_ids) > 0:
                    st.write(selected_manual_tx_ids)
                    cur.execute(
                        f"""
                                UPDATE {TX_SCHEMA}.{MANUAL_TX_TABLE}
                                set recurrence = %s
                                where id in %s
                                """,
                        (recurrence, selected_manual_tx_ids),
                    )
                    conn.commit()

        # Fetch data from the database
        # Close the connection
        cur.close()
        conn.close()


class CreditCrdCashCategoryLinking(TimeRangeView):
    def __init__(self, db_conn_str):
        super().__init__(db_conn_str=db_conn_str, months_of_history=3)

    def view_name(self):
        return "Cash Category link Credit Card transactions"

    def data_view(self, start_date: datetime.date, end_date: datetime.date) -> None:
        conn = psycopg2.connect(self.db_conn_str)
        cur = conn.cursor()

        cur.execute(
            f"SELECT id, category, subcategory FROM {TX_SCHEMA}.{TX_CATEGORY_TABLE} order by category, subcategory"
        )
        tx_category_data = cur.fetchall()
        existing_tx_categories_df = pd.DataFrame(
            tx_category_data, columns=["id", "category", "subcategory"]
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

        def _color_unlabelled_tx(value: Optional[str]):
            if value is None:
                return "background-color: indigo"

        styled_credit_transactions = credit_transactions_df.style.map(
            _color_unlabelled_tx, subset=["category"]
        )

        col_left, col_right = st.columns([0.8, 0.2])
        with col_left:
            selected_credit_transactions = st.dataframe(
                styled_credit_transactions,
                on_select="rerun",
                use_container_width=True,
                selection_mode="multi-row",
                column_config={"_index": None},
                key="credit_transactions",
                height=1200,
            )

        with col_right:
            selected_tx_category = st.dataframe(
                existing_tx_categories_df,
                on_select="rerun",
                use_container_width=True,
                selection_mode="single-row",
                column_config={"_index": None},
                key="existing_tx_categories",
                height=1200,
            )

        selected_credit_statements = []
        if selected_credit_transactions is not None:
            selected_credit_statement_rows = credit_transactions_df.iloc[
                selected_credit_transactions["selection"]["rows"]
            ]
            selected_credit_statements = [
                (t[1], int(t[2]))
                for t in selected_credit_statement_rows[
                    ["statement_file_name", "statement_id_in_file"]
                ].itertuples()
            ]
            selected_credit_statements = tuple(selected_credit_statements)
        if st.button("Link Cash Category"):
            selected_tx_category_id = None
            if (
                selected_tx_category is not None
                and len(selected_tx_category["selection"]["rows"]) > 0
            ):
                selected_tx_category_id = existing_tx_categories_df.iloc[
                    selected_tx_category["selection"]["rows"][0]
                ]["id"]
                st.write(selected_credit_statements)
                cur.execute(
                    f"""
                            UPDATE {TX_SCHEMA}.{CREDIT_CRD_TX_TABLE}
                            set tx_category = %s
                            where (statement_file_name, statement_id_in_file) in %s
                            """,
                    (int(selected_tx_category_id), selected_credit_statements),
                )
                conn.commit()

        edit_col_1, edit_col_2 = st.columns(2, gap="large")
        with edit_col_1:
            remarks_content = st.text_input("Remarks", value=None)
            if st.button("Set Remarks"):
                if len(selected_credit_statements) > 0:
                    st.write(selected_credit_statements)
                    cur.execute(
                        f"""
                                UPDATE {TX_SCHEMA}.{CREDIT_CRD_TX_TABLE}
                                set remarks = %s
                                where (statement_file_name, statement_id_in_file) in %s
                                """,
                        (remarks_content, selected_credit_statements),
                    )
                    conn.commit()

        with edit_col_2:
            recurrence = st.selectbox(
                "Recurrence Hz", options=[None, "Monthly", "Yearly"]
            )
            if st.button("Set Recurrence Hz"):
                if len(selected_credit_statements) > 0:
                    st.write(selected_credit_statements)
                    cur.execute(
                        f"""
                                UPDATE {TX_SCHEMA}.{CREDIT_CRD_TX_TABLE}
                                set recurrence = %s
                                where (statement_file_name, statement_id_in_file) in %s
                                """,
                        (recurrence, selected_credit_statements),
                    )
                    conn.commit()

        # Fetch data from the database
        # Close the connection
        cur.close()
        conn.close()
