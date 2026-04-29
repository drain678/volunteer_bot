from aiogram.fsm.state import State, StatesGroup


class ParticipationReviewState(StatesGroup):
    reject_reason_input = State()
