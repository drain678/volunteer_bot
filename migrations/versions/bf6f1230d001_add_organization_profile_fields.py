"""add organization profile fields

Revision ID: bf6f1230d001
Revises: 7d2c6b2f3a10
Create Date: 2026-04-27 14:55:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = "bf6f1230d001"
down_revision: Union[str, Sequence[str], None] = "7d2c6b2f3a10"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column(
        "organizations",
        sa.Column("representative_name", sa.String(), nullable=True),
        schema="public",
    )
    op.add_column(
        "organizations",
        sa.Column("representative_phone", sa.String(), nullable=True),
        schema="public",
    )
    op.add_column(
        "organizations",
        sa.Column("website", sa.String(), nullable=True),
        schema="public",
    )


def downgrade() -> None:
    op.drop_column("organizations", "website", schema="public")
    op.drop_column("organizations", "representative_phone", schema="public")
    op.drop_column("organizations", "representative_name", schema="public")
