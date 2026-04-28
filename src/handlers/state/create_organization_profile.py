from aiogram.fsm.state import State, StatesGroup


class OrganizationProfileState(StatesGroup):
    representative_name = State()
    representative_phone = State()
    website = State()
    description = State()
