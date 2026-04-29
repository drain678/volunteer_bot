from aiogram.fsm.state import State, StatesGroup


class OrganizationProfileState(StatesGroup):
    organization_name = State()
    representative_name = State()
    representative_phone = State()
    website = State()
    description = State()
    city = State()
    direction_select = State()
    direction_more = State()
    type_select = State()
    moderation_reject_reason = State()
