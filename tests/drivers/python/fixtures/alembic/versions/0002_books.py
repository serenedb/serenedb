import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision = "0002"
down_revision = "0001"
branch_labels = None
depends_on = None


def upgrade():
    op.create_table(
        "books",
        sa.Column("id", sa.BigInteger, primary_key=True),
        sa.Column("author_id", sa.Integer, nullable=False),
        sa.Column("title", sa.Text, nullable=False),
        sa.Column("tags", postgresql.ARRAY(sa.Text)),
        sa.Column("meta", sa.JSON),
        sa.Column("uid", sa.Uuid),
        sa.Column("cover", sa.LargeBinary),
        sa.Column("reading", sa.Interval),
        sa.Column("rating", sa.Double),
        sa.Column("pages", sa.Integer),
        sa.Column("published", sa.Date, nullable=False),
        sa.ForeignKeyConstraint(["author_id"], ["authors.id"],
                                name="books_author_id_fkey"),
        sa.CheckConstraint("rating >= 0", name="books_rating_check"),
        sa.UniqueConstraint("author_id", "title", name="books_author_title_key"),
    )
    op.create_index("books_published_idx", "books", ["published"])
    op.bulk_insert(sa.table("authors", sa.column("name", sa.Text)),
                   [{"name": "Pudge"}, {"name": "Lina"}])


def downgrade():
    op.drop_index("books_published_idx", table_name="books")
    op.drop_table("books")
    op.execute("DELETE FROM authors")
