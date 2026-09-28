from __future__ import annotations

import decimal
import os

import pytest
from spec_loader import conn_kwargs

sqlalchemy = pytest.importorskip("sqlalchemy")

from sqlalchemy import (
    ForeignKey,
    Integer,
    Numeric,
    String,
    create_engine,
    inspect,
    select,
)
from sqlalchemy.orm import (
    DeclarativeBase,
    Mapped,
    Session,
    mapped_column,
    relationship,
)

SUFFIX = os.environ.get("SDB_DRV_RUN_ID", "0")
ORDERS = f"drv_sqla_orders_{SUFFIX}"
ITEMS = f"drv_sqla_items_{SUFFIX}"


class Base(DeclarativeBase):
    pass


class Order(Base):
    __tablename__ = ORDERS
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    customer: Mapped[str] = mapped_column(String(50))
    items: Mapped[list["Item"]] = relationship(back_populates="order")


class Item(Base):
    __tablename__ = ITEMS
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    order_id: Mapped[int] = mapped_column(ForeignKey(f"{ORDERS}.id"))
    name: Mapped[str] = mapped_column(String(50))
    price: Mapped[decimal.Decimal | None] = mapped_column(Numeric(10, 2))
    order: Mapped[Order] = relationship(back_populates="items")


@pytest.fixture()
def engine():
    kw = conn_kwargs()
    engine = create_engine(
        f"postgresql+psycopg2://{kw['user']}@{kw['host']}:{kw['port']}/{kw['dbname']}"
    )
    Base.metadata.drop_all(engine)
    yield engine
    Base.metadata.drop_all(engine)
    engine.dispose()


def test_create_all_sees_existing_tables(engine):
    Base.metadata.create_all(engine)
    Base.metadata.create_all(engine)
    names = inspect(engine).get_table_names()
    assert ORDERS in names
    assert ITEMS in names


def test_reflection(engine):
    Base.metadata.create_all(engine)
    inspector = inspect(engine)
    columns = {c["name"]: c for c in inspector.get_columns(ITEMS)}
    assert list(columns) == ["id", "order_id", "name", "price"]
    assert columns["price"]["nullable"]
    assert not columns["id"]["nullable"]
    assert inspector.get_pk_constraint(ITEMS)["constrained_columns"] == ["id"]
    (fk,) = inspector.get_foreign_keys(ITEMS)
    assert fk["referred_table"] == ORDERS
    assert fk["constrained_columns"] == ["order_id"]
    assert fk["referred_columns"] == ["id"]


def test_orm_round_trip(engine):
    Base.metadata.create_all(engine)
    with Session(engine) as session:
        order = Order(customer="ann")
        order.items = [
            Item(name="pen", price=decimal.Decimal("1.50")),
            Item(name="ink", price=None),
        ]
        session.add(order)
        session.commit()
        order_id = order.id
    with Session(engine) as session:
        order = session.scalars(select(Order).where(Order.id == order_id)).one()
        assert order.customer == "ann"
        assert sorted((i.name, i.price) for i in order.items) == [
            ("ink", None),
            ("pen", decimal.Decimal("1.50")),
        ]
