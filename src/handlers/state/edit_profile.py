from aiogram.fsm.state import State, StatesGroup


class EditProfileState(StatesGroup):
    waiting_new_value = State()
    preferred_city_input = State()
    preferred_city_more = State()
    preferred_direction_select = State()
    preferred_direction_more = State()
