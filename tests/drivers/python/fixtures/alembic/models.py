import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

metadata = sa.MetaData()

sa.Table(
    "authors", metadata,
    sa.Column("id", sa.Integer, primary_key=True),
    sa.Column("name", sa.Text, nullable=False),
    sa.Column("email", sa.Text, unique=True),
    sa.Column("mood", sa.Enum("sad", "ok", "happy", name="alembic_mood")),
    sa.Column("born", sa.Date),
    sa.Column("score", sa.Numeric(10, 2), server_default="0"),
    sa.Column("active", sa.Boolean, server_default=sa.text("true")),
    sa.Column("created", sa.DateTime(timezone=True), server_default=sa.func.now()),
    comment="people who write",
)

sa.Table(
    "books", metadata,
    sa.Column("id", sa.BigInteger, primary_key=True),
    sa.Column("author_id", sa.Integer, sa.ForeignKey("authors.id"), nullable=False),
    sa.Column("title", sa.Text, nullable=False),
    sa.Column("tags", postgresql.ARRAY(sa.Text)),
    sa.Column("meta", sa.JSON),
    sa.Column("uid", sa.Uuid),
    sa.Column("cover", sa.LargeBinary),
    sa.Column("reading", sa.Interval),
    sa.Column("rating", sa.Double),
    sa.Column("pages", sa.Integer),
    sa.Column("published", sa.Date, nullable=False),
    sa.Column("blurb", sa.Text, comment="back cover"),
    sa.CheckConstraint("rating >= 0", name="books_rating_check"),
    sa.UniqueConstraint("author_id", "title", name="books_author_title_key"),
    sa.Index("books_published_idx", "published"),
    sa.Index("books_blurb_idx", "blurb"),
)

typed_metadata = sa.MetaData()

sa.Table(
    "typed", typed_metadata,
    sa.Column("id", sa.Integer, primary_key=True),
    sa.Column("isbn", sa.String(13)),
    sa.Column("code", sa.CHAR(4)),
    sa.Column("taken", postgresql.TIMESTAMP(precision=3)),
    sa.Column("price", sa.Numeric(8, 2)),
    sa.Column("ratio", sa.Numeric),
)
