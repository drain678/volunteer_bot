from aiogram.fsm.state import State, StatesGroup


class VolunteerProfileState(StatesGroup):
    name = State()
    age = State()
    city = State()
    phone = State()
    gender = State()
    preferred_city_input = State()
    preferred_city_more = State()
    preferred_direction_select = State()
    preferred_direction_more = State()
