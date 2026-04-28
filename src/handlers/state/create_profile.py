from aiogram.fsm.state import State, StatesGroup


class VolunteerProfileState(StatesGroup):
    name = State()
    age = State()
    city = State()
    gender = State()
