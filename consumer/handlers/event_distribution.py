from typing import Any, Dict

# from consumer.handlers.change_form import change_form
from consumer.handlers.create_profile import create_profile
from consumer.handlers.create_organization_profile import create_organization_profile
from consumer.handlers.delete_profile import delete_profile
from consumer.handlers.delete_organization import delete_organization
# from consumer.handlers.get_popular_users import get_top_popular_users
from consumer.handlers.get_profile import get_profile
from consumer.handlers.get_organization import get_organization
from consumer.handlers.update_profile import update_profile
from consumer.handlers.update_organization import update_organization


async def handle_event_distribution(body: Dict[str, Any]) -> None:
    match body["action"]:
        case "make_form":
            await create_profile(body)
        case "make_organization_form":
            await create_organization_profile(body)
        case "get_profile":
            await get_profile(body)
        case "get_organization":
            await get_organization(body)
        case "delete_profile":
            await delete_profile(body)
        case "update_profile":
            await update_profile(body)
        case "update_organization":
            await update_organization(body)
        case "delete_organization":
            await delete_organization(body)
        # case "find_pair":
        #     await find_candidates(body)
        # case "like_user":
        #     await process_like_user(body)
        
        # case "check_likes":
        #     await process_check_likes(body)
        # case "delete_profile":
        #     await delete_profile(body)
        # case "update_form":
        #     await change_form(body)
        # case "get_my_matches":
        #     await get_my_matches(body)
        # case 'rating':
        #     await get_top_popular_users(body)
