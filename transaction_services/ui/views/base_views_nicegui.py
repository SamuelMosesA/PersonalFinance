from abc import ABC, abstractmethod
import datetime
from typing import Optional
from nicegui import ui

class BaseNiceGUIView(ABC):
    def __init__(self, db_conn_str: str):
        super().__init__()
        self.db_conn_str = db_conn_str

    @abstractmethod
    def view_name(self) -> str:
        """Returns the name of the view."""
        ...

    @abstractmethod
    def render(self, container: ui.element) -> None:
        """Renders the view within the given container."""
        ...

class TimeRangeNiceGUIView(BaseNiceGUIView):
    def __init__(self, db_conn_str: str, months_of_history: int):
        super().__init__(db_conn_str=db_conn_str)
        self.n_history_months = max(months_of_history, 1)
        self.start_date: Optional[datetime.date] = None
        self.end_date: Optional[datetime.date] = None

    def _get_default_start_date(self, end_date: datetime.date) -> datetime.date:
        from dateutil.relativedelta import relativedelta
        return (end_date - relativedelta(months=self.n_history_months - 1)).replace(day=1)

    @abstractmethod
    def data_view(self, container: ui.element, start_date: datetime.date, end_date: datetime.date) -> None:
        """Renders the data-specific part of the view."""
        ...

    def render(self, container: ui.element) -> None:
        with container:
            with ui.column().classes('w-full mb-8'):
                ui.label(self.view_name()).classes('text-3xl font-bold text-slate-900 tracking-tight')
                ui.label('Detailed insights and management').classes('text-slate-500 text-sm')
            
            with ui.element('div').classes('p-6 mb-8 glass-card w-full bg-white border border-slate-200 shadow-sm'):
                with ui.row().classes('items-center gap-6'):
                    end_date_obj = datetime.date.today()
                    start_date_obj = self._get_default_start_date(end_date_obj)
                    
                    # Start Date Input
                    with ui.input('Tx From', value=start_date_obj.isoformat()).classes('flex-grow') as start_input:
                        start_input.props('outlined dense')
                        with start_input.add_slot('append'):
                            ui.icon('calendar_today').on('click', lambda: menu_from.open()).classes('cursor-pointer text-indigo-500')
                        with ui.menu() as menu_from:
                            ui.date().bind_value(start_input)

                    # End Date Input
                    with ui.input('Tx To', value=end_date_obj.isoformat()).classes('flex-grow') as end_input:
                        end_input.props('outlined dense')
                        with end_input.add_slot('append'):
                            ui.icon('event').on('click', lambda: menu_to.open()).classes('cursor-pointer text-indigo-500')
                        with ui.menu() as menu_to:
                            ui.date().bind_value(end_input)

                    ui.button('Refresh', on_click=lambda: self.update_data_view(data_container, start_input.value, end_input.value)).props('unelevated icon=refresh').classes('bg-indigo-600 text-white px-8 py-2 rounded-lg font-medium hover:bg-indigo-700 transition-colors shadow-sm')

            data_container = ui.column().classes('w-full gap-6')
            self.update_data_view(data_container, start_input.value, end_input.value)

    def update_data_view(self, container: ui.element, start_val: str, end_val: str) -> None:
        container.clear()
        try:
            s_date = datetime.datetime.strptime(start_val, '%Y-%m-%d').date() if isinstance(start_val, str) else start_val
            e_date = datetime.datetime.strptime(end_val, '%Y-%m-%d').date() if isinstance(end_val, str) else end_val
            self.data_view(container, s_date, e_date)
        except (ValueError, TypeError) as e:
            with container:
                ui.label(f'Invalid date format: {e}. Please use YYYY-MM-DD.').classes('text-negative text-h6')
