from aiogram.fsm.state import State, StatesGroup


class OrganizationProfileState(StatesGroup):
    organization_name = State()
    representative_name = State()
    representative_phone = State()
    website = State()
    description = State()
