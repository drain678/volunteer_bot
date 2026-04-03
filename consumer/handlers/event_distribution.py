from typing import Any, Dict

from consumer.handlers.change_form import change_form
from consumer.handlers.create_form import create_form
from consumer.handlers.delete_profile import delete_profile
from consumer.handlers.find_candidates import find_candidates
from consumer.handlers.get_likes import process_check_likes
from consumer.handlers.get_popular_users import get_top_popular_users
from consumer.handlers.get_profile import get_profile
from consumer.handlers.like_user import process_like_user
from consumer.handlers.watch_matches import get_my_matches


async def handle_event_distribution(body: Dict[str, Any]) -> None:
    match body["action"]:
        case "make_form":
            await create_form(body)
        case "find_pair":
            await find_candidates(body)
        case "like_user":
            await process_like_user(body)
        case "get_profile":
            await get_profile(body)
        case "check_likes":
            await process_check_likes(body)
        case "delete_profile":
            await delete_profile(body)
        case "update_form":
            await change_form(body)
        case "get_my_matches":
            await get_my_matches(body)
        case 'rating':
            await get_top_popular_users(body)
