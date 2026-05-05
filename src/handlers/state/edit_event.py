from aiogram.fsm.state import State, StatesGroup


class EditEventState(StatesGroup):
    choosing_field = State()
    waiting_new_value = State()
    confirm_more = State()
