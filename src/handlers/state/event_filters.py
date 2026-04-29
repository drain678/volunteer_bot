from aiogram.fsm.state import State, StatesGroup


class EventFilterState(StatesGroup):
    city_input = State()
    date_input = State()
