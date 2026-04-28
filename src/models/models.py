from datetime import datetime

from sqlalchemy import ForeignKey

from sqlalchemy.orm import Mapped, mapped_column, relationship

from src.models.meta import Base


class User(Base):
    __tablename__ = "users"

    id: Mapped[int] = mapped_column(primary_key=True)
    telegram_id: Mapped[int] = mapped_column(unique=True, index=True)

    name: Mapped[str] = mapped_column(nullable=True)
    age: Mapped[int] = mapped_column(nullable=False)
    gender: Mapped[str] = mapped_column(nullable=True)
    city: Mapped[str] = mapped_column(nullable=True)

    profile_filled: Mapped[bool] = mapped_column(default=False)

    # volunteer / organizer / admin
    role: Mapped[str] = mapped_column(default="volunteer")
    is_blocked: Mapped[bool] = mapped_column(default=False)

    created_at: Mapped[datetime] = mapped_column(default=datetime.utcnow)

    participations = relationship("Participation", back_populates="user")

    def to_dict(self) -> dict:
        return {
            "id": str(self.id),
            "name": self.name,
            "age": str(self.age),
            "gender": self.gender,
            "city": self.city,
            "profile_filled": self.profile_filled,
            "role": self.role,
            "is_blocked": self.is_blocked,
            "created_at": self.created_at.isoformat(),
        }


class Organization(Base):
    __tablename__ = "organizations"

    id: Mapped[int] = mapped_column(primary_key=True)
    name: Mapped[str]
    description: Mapped[str]
    representative_name: Mapped[str] = mapped_column(nullable=True)
    representative_phone: Mapped[str] = mapped_column(nullable=True)
    website: Mapped[str] = mapped_column(nullable=True)

    created_by: Mapped[int] = mapped_column(
        ForeignKey("public.users.id", ondelete="CASCADE")
    )

    events = relationship("Event", back_populates="organization")


class Category(Base):
    __tablename__ = "categories"

    id: Mapped[int] = mapped_column(primary_key=True)
    name: Mapped[str]

    events = relationship("Event", back_populates="category")


class Event(Base):
    __tablename__ = "events"

    id: Mapped[int] = mapped_column(primary_key=True)

    title: Mapped[str]
    description: Mapped[str]

    city: Mapped[str]
    location: Mapped[str]

    start_time: Mapped[datetime]
    duration_hours: Mapped[float]

    organization_id: Mapped[int] = mapped_column(
        ForeignKey("public.organizations.id", ondelete="CASCADE")
    )

    category_id: Mapped[int] = mapped_column(
        ForeignKey("public.categories.id", ondelete="CASCADE")
    )

    created_by: Mapped[int] = mapped_column(
        ForeignKey("public.users.id", ondelete="CASCADE")
    )

    organization = relationship("Organization", back_populates="events")
    category = relationship("Category", back_populates="events")
    participations = relationship("Participation", back_populates="event")


class Participation(Base):
    __tablename__ = "participations"

    id: Mapped[int] = mapped_column(primary_key=True)

    user_id: Mapped[int] = mapped_column(
        ForeignKey("public.users.id", ondelete="CASCADE")
    )

    event_id: Mapped[int] = mapped_column(
        ForeignKey("public.events.id", ondelete="CASCADE")
    )

    status: Mapped[str] = mapped_column(default="pending")
    # pending / approved / rejected

    created_at: Mapped[datetime] = mapped_column(default=datetime.utcnow)

    user = relationship("User", back_populates="participations")
    event = relationship("Event", back_populates="participations")


class VolunteerStats(Base):
    __tablename__ = "volunteer_stats"

    id: Mapped[int] = mapped_column(primary_key=True)

    user_id: Mapped[int] = mapped_column(
        ForeignKey("public.users.id", ondelete="CASCADE"),
        unique=True
    )

    events_count: Mapped[int] = mapped_column(default=0)
    hours_total: Mapped[float] = mapped_column(default=0.0)

    rating: Mapped[float] = mapped_column(default=0.0)
