from aiogram.fsm.state import State, StatesGroup


class EditProfileState(StatesGroup):
    waiting_new_value = State()
