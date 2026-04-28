from aiogram.fsm.state import State, StatesGroup


class EditOrganizationState(StatesGroup):
    waiting_new_value = State()
