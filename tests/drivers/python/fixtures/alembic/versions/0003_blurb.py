import sqlalchemy as sa
from alembic import op

revision = "0003"
down_revision = "0002"
branch_labels = None
depends_on = None


def upgrade():
    op.add_column("books", sa.Column("summary", sa.Text))
    op.alter_column("books", "summary", new_column_name="blurb")
    op.create_index("books_blurb_idx", "books", ["blurb"])
    op.alter_column("books", "blurb", comment="back cover", existing_type=sa.Text)


def downgrade():
    op.drop_index("books_blurb_idx", table_name="books")
    op.drop_column("books", "blurb")
