from aiogram.fsm.state import State, StatesGroup


class OrganizationFilterState(StatesGroup):
    city_input = State()
