"""add gender to users

Revision ID: 7d2c6b2f3a10
Revises: 91df3c224da5
Create Date: 2026-04-27 14:40:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = "7d2c6b2f3a10"
down_revision: Union[str, Sequence[str], None] = "91df3c224da5"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column("users", sa.Column("gender", sa.String(), nullable=True), schema="public")


def downgrade() -> None:
    op.drop_column("users", "gender", schema="public")
