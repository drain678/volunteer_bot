from aiogram.fsm.state import State, StatesGroup


class CreateEventState(StatesGroup):
    title = State()
    description = State()
    min_age = State()
    city = State()
    start_time = State()
    duration_hours = State()
    category = State()
