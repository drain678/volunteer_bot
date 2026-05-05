from aiogram.fsm.state import State, StatesGroup


class OrganizationBanState(StatesGroup):
    waiting_reason = State()
