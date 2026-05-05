"""admin_and_org_ban_flags

Revision ID: b1f9c6d2a110
Revises: 8de7fef80a1a
Create Date: 2026-05-05 14:59:00.000000

"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = "b1f9c6d2a110"
down_revision: Union[str, Sequence[str], None] = "8de7fef80a1a"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column(
        "users",
        sa.Column("is_admin", sa.Boolean(), nullable=False, server_default=sa.text("false")),
    )
    op.add_column(
        "organizations",
        sa.Column("is_banned", sa.Boolean(), nullable=False, server_default=sa.text("false")),
    )
    op.alter_column("users", "is_admin", server_default=None)
    op.alter_column("organizations", "is_banned", server_default=None)


def downgrade() -> None:
    op.drop_column("organizations", "is_banned")
    op.drop_column("users", "is_admin")
