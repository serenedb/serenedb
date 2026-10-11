import sqlalchemy as sa
from alembic import op

revision = "0001"
down_revision = None
branch_labels = None
depends_on = None


def upgrade():
    mood = sa.Enum("sad", "ok", "happy", name="alembic_mood")
    op.create_table(
        "authors",
        sa.Column("id", sa.Integer, primary_key=True),
        sa.Column("name", sa.Text, nullable=False),
        sa.Column("email", sa.Text),
        sa.Column("mood", mood),
        sa.Column("born", sa.Date),
        sa.Column("score", sa.Numeric(10, 2), server_default="0"),
        sa.Column("active", sa.Boolean, server_default=sa.text("true")),
        sa.Column("created", sa.DateTime(timezone=True), server_default=sa.func.now()),
        sa.UniqueConstraint("email", name="authors_email_key"),
        comment="people who write",
    )


def downgrade():
    op.drop_table("authors")
    sa.Enum(name="alembic_mood").drop(op.get_bind())
