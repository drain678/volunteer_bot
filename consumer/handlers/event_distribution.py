from typing import Any, Dict

# from consumer.handlers.change_form import change_form
from consumer.handlers.create_profile import create_profile
from consumer.handlers.create_organization_profile import create_organization_profile
from consumer.handlers.create_event import create_event
from consumer.handlers.delete_profile import delete_profile
from consumer.handlers.delete_organization import delete_organization
# from consumer.handlers.get_popular_users import get_top_popular_users
from consumer.handlers.get_profile import get_profile
from consumer.handlers.get_organization import get_organization
from consumer.handlers.get_organizations import get_organizations
from consumer.handlers.get_events import get_events
from consumer.handlers.get_my_events import get_my_events
from consumer.handlers.get_volunteer_my_events import get_volunteer_my_events
from consumer.handlers.get_event_participants import get_event_participants
from consumer.handlers.get_tops import get_tops
from consumer.handlers.participate_event import participate_event
from consumer.handlers.review_participation import review_participation
from consumer.handlers.delete_event import delete_event
from consumer.handlers.update_profile import update_profile
from consumer.handlers.update_organization import update_organization
from consumer.handlers.update_event import update_event


async def handle_event_distribution(body: Dict[str, Any]) -> None:
    match body["action"]:
        case "make_form":
            await create_profile(body)
        case "make_organization_form":
            await create_organization_profile(body)
        case "create_event":
            await create_event(body)
        case "get_profile":
            await get_profile(body)
        case "get_organization":
            await get_organization(body)
        case "get_organizations":
            await get_organizations(body)
        case "get_events":
            await get_events(body)
        case "get_my_events":
            await get_my_events(body)
        case "get_event_participants":
            await get_event_participants(body)
        case "get_tops":
            await get_tops(body)
        case "get_volunteer_my_events":
            await get_volunteer_my_events(body)
        case "participate_event":
            await participate_event(body)
        case "review_participation":
            await review_participation(body)
        case "delete_event":
            await delete_event(body)
        case "delete_profile":
            await delete_profile(body)
        case "update_profile":
            await update_profile(body)
        case "update_organization":
            await update_organization(body)
        case "update_event":
            await update_event(body)
        case "delete_organization":
            await delete_organization(body)

