from abc import ABC, abstractmethod
import datetime
import dateutil
import streamlit as st

class BaseStreamlitView(ABC):
    def __init__(self, db_conn_str: str):
        super().__init__()
        self.db_conn_str = db_conn_str

    @abstractmethod
    def view_name(self) -> str: ...

    @abstractmethod
    def render(self) -> None: ...



def _first_day_of_n_months_ago(date_to: datetime.date, n: int) -> datetime.date:
    # Get the current date
    n_months_ago = date_to - dateutil.relativedelta.relativedelta(months=n)
    # Get the first day of that month
    first_day_of_two_months_ago = n_months_ago.replace(day=1)
    return first_day_of_two_months_ago


class TimeRangeView(BaseStreamlitView):
    def __init__(self, db_conn_str:str, months_of_history:int):
        super().__init__(db_conn_str=db_conn_str)
        self.n_history_months = max(months_of_history,1)

    @abstractmethod
    def data_view(self, start_date:datetime.date, end_date:datetime.date) -> None: ...

    @st.fragment
    def data_view_fragment(self) -> None:
        if st.button("Refresh"):
            st.rerun(scope="fragment")

        tx_to_default = datetime.date.today()
        if 'tx_to' in st.session_state:
            tx_to_default = st.session_state['tx_to']


        time_select_col_1, time_select_col_2 = st.columns(
            2, gap="large", vertical_alignment="bottom"
        )
        with time_select_col_2:
            tx_to = st.date_input(
                "Tx To",
                value= tx_to_default,                 
                max_value=datetime.date.today(),
            )
        tx_from_default =  _first_day_of_n_months_ago(tx_to, self.n_history_months -1)
        with time_select_col_1:
            tx_from = st.date_input(
                "Tx From",
                value= tx_from_default,
                max_value=datetime.date.today(),
            )

        st.session_state['tx_to'] = tx_to

        self.data_view(tx_from, tx_to)

    def render(self):
        st.header(self.view_name())
        self.data_view_fragment()