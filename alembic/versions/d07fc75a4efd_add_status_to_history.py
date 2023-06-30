"""Add status to history

Revision ID: d07fc75a4efd
Revises: 67c92f53b7e5
Create Date: 2020-07-06 13:12:07.111702

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = 'd07fc75a4efd'
down_revision = '67c92f53b7e5'
branch_labels = None
depends_on = None


def upgrade():
    op.add_column('history_table', sa.Column('status', sa.String(256)))


def downgrade():
    op.drop_column('history_table', 'status')
