from aiogram.fsm.state import State, StatesGroup


class DeleteEventState(StatesGroup):
    waiting_reason = State()
