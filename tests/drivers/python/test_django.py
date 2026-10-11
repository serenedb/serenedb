from __future__ import annotations

import datetime
import decimal
import io
import json
import uuid
from pathlib import Path

import django_harness as harness
import pytest
from target import check_golden, is_serenedb

DATABASES = [harness.database_name(), harness.stock_database_name()]
SERENEDB = is_serenedb()
GOLDENS = Path(__file__).resolve().parent / "fixtures" / "django"
TABLES = ["django_drv_author", "django_drv_book", "django_drv_book_tags",
          "django_drv_review", "django_drv_tag"]


@pytest.fixture(scope="module", autouse=True)
def database():
    with harness.admin_connection() as conn:
        for name in DATABASES:
            conn.execute(f"DROP DATABASE IF EXISTS {name}")
            conn.execute(f"CREATE DATABASE {name}")
    harness.configure()
    yield
    from django.db import connections

    connections.close_all()
    connections["pooled"].close_pool()
    with harness.admin_connection() as conn:
        for name in DATABASES:
            conn.execute(f"DROP DATABASE IF EXISTS {name}")


@pytest.fixture(scope="module")
def migrated(database):
    harness.create_schema()
    for app in harness.CORE_APPS:
        call("migrate", app, fake=True, interactive=False)


def call(*args, **kwargs) -> str:
    from django.core.management import call_command

    out = io.StringIO()
    call_command(*args, stdout=out, stderr=out, verbosity=kwargs.pop("verbosity", 1),
                 **kwargs)
    return out.getvalue()


def run_operations(operations, using="default"):
    from django.db import connections
    from django.db.migrations.state import ProjectState

    state = ProjectState()
    for operation in operations:
        new_state = state.clone()
        operation.state_forwards("django_drv", new_state)
        with connections[using].schema_editor() as editor:
            operation.database_forwards("django_drv", editor, state, new_state)
        state = new_state


def plain(value):
    if isinstance(value, float):
        return round(value, 9)
    if isinstance(value, (datetime.date, datetime.time)):
        return value.isoformat()
    if isinstance(value, (decimal.Decimal, datetime.timedelta, uuid.UUID)):
        return str(value)
    if isinstance(value, (bytes, memoryview)):
        return bytes(value).hex()
    if isinstance(value, (list, tuple)):
        return [plain(item) for item in value]
    if isinstance(value, dict):
        return {key: plain(item) for key, item in value.items()}
    return value


def outcomes(probes) -> str:
    from django.db import DatabaseError

    result = {}
    for name, probe in probes.items():
        try:
            result[name] = plain(probe())
        except DatabaseError as error:
            result[name] = f"error: {type(error).__name__}"
    return json.dumps(result, indent=1) + "\n"


def library(name: str):
    from django_drv.models import Author, Book, Tag

    writer = Author.objects.create(name=name, born=datetime.date(1987, 6, 5))
    horror = Tag.objects.create(label=f"{name.lower().replace(' ', '-')}-horror")
    utc = datetime.timezone.utc
    rows = [
        ("Chronosphere 10", 10, "1.50", 4.5, True,
         datetime.datetime(2020, 1, 15, 7, 8, 9, tzinfo=utc), datetime.timedelta(hours=1)),
        ("Chronosphere 20", 20, "2.50", None, False,
         datetime.datetime(2020, 3, 15, 12, tzinfo=utc), None),
        ("time walk", 30, "3.00", 3.0, True,
         datetime.datetime(2021, 12, 31, 23, 59, 59, tzinfo=utc),
         datetime.timedelta(hours=2, minutes=30)),
        ("Time Lock", 40, "0.00", None, True, None, None),
        ("BACKTRACK", 50, "5.25", 5.0, False,
         datetime.datetime(2019, 6, 1, tzinfo=utc), datetime.timedelta(seconds=90)),
    ]
    books = Book.objects.filter(author=writer)
    for title, pages, price, rating, in_print, published, reading_time in rows:
        Book.objects.create(author=writer, title=title, pages=pages,
                            price=decimal.Decimal(price), rating=rating,
                            in_print=in_print, published=published,
                            reading_time=reading_time, isbn=title[:4].lower(),
                            cover=title.encode())
    for book in books.filter(pages__lte=20):
        book.tags.add(horror)
    return writer, books


@pytest.mark.xfail(SERENEDB, strict=True,
                   reason="SereneDB rejects the DDL of Django's PostgreSQL backend: "
                          "identity columns (#1426), ALTER TABLE ADD FOREIGN KEY "
                          "(#1427), DEFERRABLE (#1381), DROP COLUMN before an "
                          "indexed column (#1428), ALTER TYPE of a UNIQUE column "
                          "(#1429)")
def test_migrate_with_stock_backend(database):
    from django.db import connections
    from django_drv.models import Author

    for app in harness.CORE_APPS:
        call("migrate", app, database="stock", interactive=False)
    author = Author.objects.using("stock").create(name="Invoker")
    assert author.pk == 1
    with connections["stock"].cursor() as cursor:
        columns = connections["stock"].introspection.get_table_description(
            cursor, "django_drv_author")
    assert [c.name for c in columns if c.is_autofield] == ["id"]
    call("migrate", "django_drv", "zero", database="stock", interactive=False)


@pytest.mark.xfail(SERENEDB, strict=True,
                   reason="SereneDB has no identity columns (#1426), Django 4.1+ "
                          "creates auto fields as GENERATED BY DEFAULT AS IDENTITY")
def test_identity_auto_field(database):
    from django.db import migrations, models

    run_operations([
        migrations.CreateModel("Counter", [
            ("id", models.BigAutoField(primary_key=True)),
            ("hits", models.IntegerField()),
        ]),
        migrations.DeleteModel("Counter"),
    ], using="stock")


@pytest.mark.xfail(SERENEDB, strict=True,
                   reason="SereneDB cannot add a FOREIGN KEY with ALTER TABLE (#1427)")
def test_alter_field_to_foreign_key(migrated):
    from django.db import migrations, models

    run_operations([
        migrations.CreateModel("Kennel", [("id", models.BigAutoField(primary_key=True))]),
        migrations.CreateModel("Hound", [
            ("id", models.BigAutoField(primary_key=True)),
            ("kennel", models.ForeignKey("django_drv.Kennel", models.CASCADE,
                                         db_constraint=False)),
        ]),
        migrations.AlterField("Hound", "kennel", models.ForeignKey(
            "django_drv.Kennel", models.CASCADE)),
        migrations.DeleteModel("Hound"),
        migrations.DeleteModel("Kennel"),
    ])


@pytest.mark.xfail(SERENEDB, strict=True,
                   reason="SereneDB cannot add a column with a REFERENCES "
                          "constraint (#1427) and has no SET CONSTRAINTS (#1381)")
def test_add_foreign_key_field(migrated):
    from django.db import migrations, models

    run_operations([
        migrations.CreateModel("Stable", [("id", models.BigAutoField(primary_key=True))]),
        migrations.CreateModel("Pony", [("id", models.BigAutoField(primary_key=True))]),
        migrations.AddField("Pony", "stable", models.ForeignKey(
            "django_drv.Stable", models.CASCADE, null=True)),
        migrations.DeleteModel("Pony"),
        migrations.DeleteModel("Stable"),
    ])


@pytest.mark.xfail(SERENEDB, strict=True,
                   reason="SereneDB cannot drop a column that precedes an indexed "
                          "column (#1428), contenttypes.0002 drops "
                          "django_content_type.name")
def test_remove_field_before_indexed_columns(migrated):
    from django.db import migrations, models

    run_operations([
        migrations.CreateModel("Label", [
            ("id", models.BigAutoField(primary_key=True)),
            ("legacy", models.CharField(max_length=10, null=True)),
            ("app", models.CharField(max_length=10)),
            ("model", models.CharField(max_length=10)),
        ], options={"unique_together": {("app", "model")}}),
        migrations.RemoveField("Label", "legacy"),
        migrations.DeleteModel("Label"),
    ])


@pytest.mark.xfail(SERENEDB, strict=True,
                   reason="SereneDB cannot change the type of a UNIQUE column "
                          "(#1429), auth.0008 widens auth_user.username")
def test_alter_max_length_of_unique_field(migrated):
    from django.db import migrations, models

    run_operations([
        migrations.CreateModel("Handle", [
            ("id", models.BigAutoField(primary_key=True)),
            ("name", models.CharField(max_length=30, unique=True)),
        ]),
        migrations.AlterField("Handle", "name",
                              models.CharField(max_length=150, unique=True)),
        migrations.DeleteModel("Handle"),
    ])


def test_migration_state(migrated):
    shown = call("showmigrations", *harness.CORE_APPS)
    assert "[X]" in shown
    assert "[ ]" not in shown


def test_models_match_migrations(migrated):
    call("makemigrations", check=True, dry_run=True)


def test_inspectdb(migrated):
    check_golden(GOLDENS / "inspectdb.py", call("inspectdb", *TABLES))


def test_introspection(migrated):
    from django.db import connection

    introspection = connection.introspection
    result = {}
    with connection.cursor() as cursor:
        tables = {t.name: t for t in introspection.get_table_list(cursor)}
        for name in TABLES:
            table = tables[name]
            result[name] = {
                "type": table.type,
                "comment": table.comment,
                "columns": [
                    [c.name, c.type_code, c.internal_size, c.precision, c.scale,
                     c.null_ok, c.default, c.collation, c.is_autofield, c.comment]
                    for c in introspection.get_table_description(cursor, name)
                ],
                "primary_key": introspection.get_primary_key_columns(cursor, name),
                "relations": sorted(
                    [column, list(target)] for column, target in
                    introspection.get_relations(cursor, name).items()),
                "sequences": introspection.get_sequences(cursor, name),
                "constraints": {
                    key: {field: value for field, value in sorted(info.items())}
                    for key, info in sorted(
                        introspection.get_constraints(cursor, name).items())
                },
            }
    check_golden(GOLDENS / "introspection.json",
                 json.dumps(result, indent=1, sort_keys=True,
                            default=lambda value: getattr(value, "__name__", str(value)))
                 + "\n")


def test_table_names_and_views(migrated):
    from django.db import connection

    with connection.cursor() as cursor:
        cursor.execute("CREATE VIEW drv_django_titles AS "
                       "SELECT title FROM django_drv_book")
        try:
            names = connection.introspection.table_names(cursor, include_views=True)
            assert "drv_django_titles" in names
            assert "drv_django_titles" not in connection.introspection.table_names(
                cursor)
            assert set(TABLES) <= set(names)
        finally:
            cursor.execute("DROP VIEW drv_django_titles")


def test_orm_round_trip_of_every_field(migrated):
    from django_drv.models import Author, Book, Review, Tag

    author = Author.objects.create(name="Pudge", email="pudge@dota", bio="hook",
                                   born=datetime.date(2004, 2, 1))
    published = datetime.datetime(2024, 5, 6, 7, 8, 9, 123456,
                                  tzinfo=datetime.timezone.utc)
    book = Book.objects.create(
        author=author, title="Fresh Meat", pages=321,
        price=decimal.Decimal("12.50"), rating=4.5, in_print=False,
        published=published, reading_time=datetime.timedelta(hours=3, seconds=7),
        cover=b"\x00\x01binary", isbn="9780000000001")
    book.tags.add(Tag.objects.create(label="horror"),
                  Tag.objects.create(label="comedy"))
    Review.objects.create(book=book, stars=5, reviewer_ip="10.0.0.1",
                          likes=2**40, opened_at=datetime.time(13, 14, 15))
    fresh = Book.objects.get(pk=book.pk)
    assert isinstance(fresh.uid, uuid.UUID) and fresh.uid == book.uid
    assert fresh.price == decimal.Decimal("12.50")
    assert fresh.rating == 4.5 and fresh.in_print is False
    assert fresh.published == published
    assert fresh.reading_time == datetime.timedelta(hours=3, seconds=7)
    assert bytes(fresh.cover) == b"\x00\x01binary"
    assert sorted(fresh.tags.values_list("label", flat=True)) == ["comedy",
                                                                  "horror"]
    review = Review.objects.select_related("book__author").get(book=fresh)
    assert review.book.author.name == "Pudge"
    assert review.reviewer_ip == "10.0.0.1" and review.likes == 2**40
    assert review.opened_at == datetime.time(13, 14, 15)
    assert review.created is not None


def test_lookups_and_functions(migrated):
    from django.db.models import Count, F, Q
    from django.db.models.functions import (Coalesce, Concat, Extract, Length,
                                            Lower, Substr, TruncMonth, Upper)
    from django_drv.models import Author, Book

    writer = Author.objects.create(name="Crystal Maiden", born=datetime.date(1999,
                                                                               12, 31))
    for i in range(5):
        Book.objects.create(author=writer, title=f"Frostbite {i}", pages=i * 100,
                            price=decimal.Decimal(i), isbn=f"isbn{i}",
                            published=datetime.datetime(2020, i + 1, 15,
                                                        tzinfo=datetime.timezone.utc))
    books = Book.objects.filter(author=writer)
    assert books.filter(title__iexact="FROSTBITE 1").count() == 1
    assert books.filter(title__icontains="BITE").count() == 5
    assert books.filter(title__startswith="Frost", pages__gte=200).count() == 3
    assert books.filter(title__regex=r"^Frost.*[34]$").count() == 2
    assert books.filter(title__iregex=r"^frost").count() == 5
    assert books.filter(pages__in=[0, 100, 999]).count() == 2
    assert books.filter(pages__range=(100, 300)).count() == 3
    assert books.filter(rating__isnull=True).count() == 5
    assert books.filter(published__year=2020, published__month=3).count() == 1
    assert books.filter(Q(pages=0) | Q(pages=400)).count() == 2
    assert list(books.order_by("-pages").values_list("pages", flat=True)[:2]) == [
        400, 300]
    annotated = books.annotate(
        upper=Upper("title"), lower=Lower("title"), size=Length("title"),
        head=Substr("title", 1, 5), label=Concat("title", F("isbn")),
        month=Extract("published", "month"), bucket=TruncMonth("published"),
        score=Coalesce("rating", 0.0)).order_by("pages").first()
    assert annotated.upper == "FROSTBITE 0" and annotated.lower == "frostbite 0"
    assert annotated.size == 11 and annotated.head == "Frost"
    assert annotated.label == "Frostbite 0isbn0" and annotated.month == 1
    assert annotated.bucket == datetime.datetime(2020, 1, 1,
                                                 tzinfo=datetime.timezone.utc)
    assert annotated.score == 0.0
    assert books.aggregate(n=Count("id"))["n"] == 5
    books.filter(pages=0).update(pages=F("pages") + 7)
    assert books.get(title="Frostbite 0").pages == 7


@pytest.mark.xfail(SERENEDB, strict=True,
                   reason="SereneDB wants every selected column in GROUP BY "
                          "(#1432), PostgreSQL lets a grouped primary key cover the "
                          "other columns of its table")
def test_group_by_primary_key(migrated):
    from django.db.models import Count
    from django_drv.models import Author, Book

    writer = Author.objects.create(name="Mirana")
    Book.objects.create(author=writer, title="Moonlight", price=1, isbn="m")
    assert Author.objects.annotate(n=Count("books")).get(pk=writer.pk).n == 1


def test_aggregates_windows_and_subqueries(migrated):
    from django.db.models import (Avg, Count, Exists, Max, Min, OuterRef,
                                  StdDev, Subquery, Sum, Variance, Window)
    from django.db.models.functions import Lag, Rank, RowNumber
    from django_drv.models import Author, Book

    writer = Author.objects.create(name="Shadow Fiend")
    for pages in (10, 20, 20, 50):
        Book.objects.create(author=writer, title=f"Requiem {pages} {Book.objects.count()}",
                            pages=pages, price=decimal.Decimal("1.00"),
                            isbn="r")
    books = Book.objects.filter(author=writer)
    totals = books.aggregate(total=Sum("pages"), avg=Avg("pages"),
                             low=Min("pages"), high=Max("pages"),
                             spread=StdDev("pages"), var=Variance("pages"))
    assert totals["total"] == 100 and totals["avg"] == 25
    assert totals["low"] == 10 and totals["high"] == 50
    assert round(totals["var"], 6) == 225.0
    ranked = list(books.annotate(
        rank=Window(Rank(), order_by="pages"),
        row=Window(RowNumber(), order_by=["pages", "id"]),
        previous=Window(Lag("pages"), order_by=["pages", "id"]),
    ).order_by("pages", "id").values_list("pages", "rank", "row", "previous"))
    assert ranked == [(10, 1, 1, None), (20, 2, 2, 10), (20, 2, 3, 20),
                      (50, 4, 4, 20)]
    newest = Book.objects.filter(author=OuterRef("pk")).order_by("-pages")
    authors = Author.objects.filter(pk=writer.pk).annotate(
        best=Subquery(newest.values("pages")[:1]),
        has_books=Exists(Book.objects.filter(author=OuterRef("pk"))))
    assert authors.get().best == 50 and authors.get().has_books
    assert books.values("pages").annotate(n=Count("id")).order_by(
        "pages").filter(n__gt=1).get()["pages"] == 20
    assert books.distinct("pages").order_by("pages").count() == 3
    union = books.filter(pages=10).union(books.filter(pages=50))
    assert sorted(b.pages for b in union) == [10, 50]


def test_bulk_operations(migrated):
    from django_drv.models import Author, Tag

    created = Tag.objects.bulk_create([Tag(label=f"bulk-{i}") for i in range(50)])
    assert all(tag.pk for tag in created)
    for tag in created:
        tag.label = tag.label.upper()
    Tag.objects.bulk_update(created, ["label"])
    assert Tag.objects.filter(label__startswith="BULK-").count() == 50
    obj, made = Author.objects.get_or_create(name="Lina", defaults={"email": "a@b"})
    assert made
    again, made_again = Author.objects.get_or_create(name="Lina")
    assert again.pk == obj.pk and not made_again
    assert set(Tag.objects.in_bulk([created[0].pk, created[1].pk])) == {
        created[0].pk, created[1].pk}
    assert Tag.objects.filter(label__startswith="BULK-").delete()[0] == 50


@pytest.mark.xfail(SERENEDB, strict=True,
                   reason="SereneDB has no DECLARE CURSOR (#720), QuerySet.iterator() "
                          "streams through a server-side cursor")
def test_iterator_streams_rows(migrated):
    from django_drv.models import Tag

    Tag.objects.bulk_create([Tag(label=f"iter-{i}") for i in range(30)])
    labels = [t.label for t in Tag.objects.using("server_cursors")
              .filter(label__startswith="iter-").order_by("id").iterator(chunk_size=7)]
    assert len(labels) == 30


def test_iterator_without_server_side_cursors(migrated):
    from django_drv.models import Tag

    Tag.objects.bulk_create([Tag(label=f"client-{i}") for i in range(30)])
    labels = [t.label for t in Tag.objects.filter(label__startswith="client-")
              .order_by("id").iterator(chunk_size=7)]
    assert len(labels) == 30


@pytest.mark.xfail(SERENEDB, strict=True,
                   reason="SereneDB has no SAVEPOINT (#722), a nested "
                          "transaction.atomic() is a savepoint")
def test_savepoints(migrated):
    from django.db import IntegrityError, transaction
    from django_drv.models import Author

    with transaction.atomic():
        Author.objects.create(name="Earthshaker")
        with pytest.raises(IntegrityError):
            with transaction.atomic():
                Author.objects.create(name="Earthshaker")
        Author.objects.create(name="Earthshaker 2")
    assert Author.objects.filter(name__startswith="Earthshaker").count() == 2


def test_transactions(migrated):
    from django.db import IntegrityError, transaction
    from django_drv.models import Author

    Author.objects.create(name="Slardar")
    with pytest.raises(IntegrityError):
        with transaction.atomic():
            Author.objects.create(name="Slardar")
    assert Author.objects.filter(name="Slardar").count() == 1
    committed = []
    with transaction.atomic():
        transaction.on_commit(lambda: committed.append(True))
        Author.objects.create(name="Tiny")
    assert committed == [True]
    try:
        with transaction.atomic():
            Author.objects.create(name="Rolled Back")
            raise RuntimeError
    except RuntimeError:
        pass
    assert not Author.objects.filter(name="Rolled Back").exists()


@pytest.mark.xfail(SERENEDB, strict=True,
                   reason="SereneDB has no SELECT ... FOR UPDATE (#1431), which "
                          "select_for_update() and update_or_create() send")
def test_select_for_update(migrated):
    from django.db import transaction
    from django_drv.models import Author

    Author.objects.create(name="Axe")
    with transaction.atomic():
        locked = Author.objects.select_for_update().get(name="Axe")
        locked.email = "axe@dota"
        locked.save()
    assert Author.objects.get(name="Axe").email == "axe@dota"
    updated, created = Author.objects.update_or_create(
        name="Axe", defaults={"email": "culling@dota"})
    assert not created and updated.email == "culling@dota"


def test_check_constraint_is_enforced(migrated):
    from django.db import IntegrityError, transaction
    from django_drv.models import Author, Book

    writer = Author.objects.create(name="Necrophos")
    with pytest.raises(IntegrityError):
        with transaction.atomic():
            Book.objects.create(author=writer, title="Debt", pages=1,
                                price=decimal.Decimal("-1"), isbn="x")


@pytest.mark.xfail(SERENEDB, strict=True,
                   reason="SereneDB rejects deleting a referenced row in the "
                          "transaction that deleted the rows referencing it "
                          "(#1433), Django's collector deletes children before "
                          "parents")
def test_foreign_key_cascade(migrated):
    from django_drv.models import Author, Book, Review

    writer = Author.objects.create(name="Undying")
    book = Book.objects.create(author=writer, title="Tombstone", price=1, isbn="t")
    Review.objects.create(book=book, stars=1)
    writer.delete()
    assert not Book.objects.filter(pk=book.pk).exists()
    assert not Review.objects.filter(book_id=book.pk).exists()


def test_raw_sql_and_cursor(migrated):
    from django.db import connection
    from django_drv.models import Author

    Author.objects.create(name="Lich")
    raw = list(Author.objects.raw("SELECT * FROM django_drv_author WHERE name = %s",
                                  ["Lich"]))
    assert raw[0].name == "Lich"
    with connection.cursor() as cursor:
        cursor.execute("SELECT %s::int + %s::int", [2, 3])
        assert cursor.fetchone() == (5,)


def test_explain(migrated):
    from django_drv.models import Book

    assert Book.objects.filter(pages__gt=1).explain()


def test_schema_editor_operations(migrated):
    from django.db import connection, migrations, models

    run_operations([
        migrations.CreateModel("Gadget", [
            ("id", models.BigAutoField(primary_key=True)),
            ("name", models.CharField(max_length=20)),
            ("weight", models.IntegerField(default=1)),
        ]),
        migrations.AddField("Gadget", "color",
                            models.CharField(max_length=10, default="red")),
        migrations.AlterField("Gadget", "weight", models.BigIntegerField(default=1)),
        migrations.AlterField("Gadget", "weight", models.BigIntegerField(null=True)),
        migrations.AlterField("Gadget", "name", models.CharField(max_length=60)),
        migrations.RenameField("Gadget", "color", "colour"),
        migrations.AddIndex("Gadget", models.Index(fields=["name"],
                                                   name="drv_gadget_name_idx")),
        migrations.AddConstraint("Gadget", models.UniqueConstraint(
            fields=["name", "colour"], name="drv_gadget_name_colour_uniq")),
        migrations.AddConstraint("Gadget", models.CheckConstraint(
            condition=models.Q(weight__gte=0), name="drv_gadget_weight_check")),
        migrations.RemoveConstraint("Gadget", "drv_gadget_weight_check"),
        migrations.RemoveIndex("Gadget", "drv_gadget_name_idx"),
        migrations.RemoveConstraint("Gadget", "drv_gadget_name_colour_uniq"),
        migrations.RemoveField("Gadget", "colour"),
        migrations.AlterModelTable("Gadget", "drv_gadgets"),
        migrations.RenameModel("Gadget", "Widget"),
        migrations.DeleteModel("Widget"),
    ])
    with connection.cursor() as cursor:
        names = connection.introspection.table_names(cursor)
    assert "drv_gadgets" not in names and "django_drv_gadget" not in names


@pytest.mark.xfail(SERENEDB, strict=True,
                   reason="SereneDB runs one subcommand per ALTER TABLE (#1430), "
                          "Django changes a column's type and nullability in one "
                          "statement")
def test_alter_type_and_nullability_together(migrated):
    from django.db import migrations, models

    run_operations([
        migrations.CreateModel("Crate", [
            ("id", models.BigAutoField(primary_key=True)),
            ("weight", models.IntegerField()),
        ]),
        migrations.AlterField("Crate", "weight", models.BigIntegerField(null=True)),
        migrations.DeleteModel("Crate"),
    ])


@pytest.mark.xfail(SERENEDB, strict=True,
                   reason="SereneDB cannot drop a column of a UNIQUE constraint "
                          "(#1428), PostgreSQL drops the constraint with it")
def test_remove_field_in_unique_constraint(migrated):
    from django.db import migrations, models

    run_operations([
        migrations.CreateModel("Badge", [
            ("id", models.BigAutoField(primary_key=True)),
            ("owner", models.CharField(max_length=10)),
            ("kind", models.CharField(max_length=10)),
        ], options={"constraints": [models.UniqueConstraint(
            fields=["owner", "kind"], name="drv_badge_owner_kind_uniq")]}),
        migrations.RemoveField("Badge", "kind"),
        migrations.DeleteModel("Badge"),
    ])


@pytest.mark.xfail(SERENEDB, strict=True,
                   reason="SereneDB has no DECLARE CURSOR (#720), dumpdata reads "
                          "through a server-side cursor")
def test_dumpdata(migrated):
    from django_drv.models import Tag

    Tag.objects.create(label="dump-me")
    assert "dump-me" in call("dumpdata", "django_drv.tag", format="json",
                             database="server_cursors", verbosity=0)


def test_dumpdata_without_server_side_cursors(migrated):
    from django_drv.models import Tag

    Tag.objects.create(label="dump-me-too")
    assert "dump-me-too" in call("dumpdata", "django_drv.tag", format="json",
                                 verbosity=0)


def test_management_commands(migrated):
    from django_drv.models import Tag

    assert "setval" in call("sqlsequencereset", "django_drv")
    call("createcachetable")
    from django.core.cache import cache

    cache.set("hook", "fresh meat", 30)
    assert cache.get("hook") == "fresh meat"
    call("flush", interactive=False)
    assert Tag.objects.count() == 0


@pytest.mark.xfail(SERENEDB, strict=True,
                   reason="SereneDB has no SET CONSTRAINTS (#1381), loaddata checks "
                          "the loaded rows with SET CONSTRAINTS ALL IMMEDIATE")
def test_loaddata(migrated):
    from django_drv.models import Tag

    call("loaddata", str(GOLDENS / "tags.json"))
    assert Tag.objects.filter(label__in=["loaded-a", "loaded-b"]).count() == 2


def test_database_functions(migrated):
    from django.db.models import (CharField, DateField, DateTimeField,
                                  ExpressionWrapper, F, FloatField, IntegerField,
                                  TextField, Value)
    from django.db.models import functions as fn

    _, books = library("Faceless Void")
    book = books.get(pages=10)
    functions = {
        "upper": fn.Upper("title"),
        "lower": fn.Lower("title"),
        "length": fn.Length("title"),
        "substr": fn.Substr("title", 2, 4),
        "left": fn.Left("title", 3),
        "right": fn.Right("title", 3),
        "lpad": fn.LPad("isbn", 8, Value("*")),
        "rpad": fn.RPad("isbn", 8, Value("*")),
        "ltrim": fn.LTrim(Value("  void  ")),
        "rtrim": fn.RTrim(Value("  void  ")),
        "trim": fn.Trim(Value("  void  ")),
        "repeat": fn.Repeat(Value("ab"), 3),
        "replace": fn.Replace("title", Value("sphere"), Value("lock")),
        "reverse": fn.Reverse("title"),
        "strindex": fn.StrIndex("title", Value("sphere")),
        "chr": fn.Chr(Value(80)),
        "ord": fn.Ord("title"),
        "md5": fn.MD5("title"),
        "sha1": fn.SHA1("title"),
        "sha224": fn.SHA224("title"),
        "sha256": fn.SHA256("title"),
        "sha384": fn.SHA384("title"),
        "sha512": fn.SHA512("title"),
        "concat": fn.Concat("title", Value("-"), "isbn", output_field=TextField()),
        "collate": fn.Collate("title", "C"),
        "abs": fn.Abs(Value(-5)),
        "ceil": fn.Ceil("rating"),
        "floor": fn.Floor("rating"),
        "round": fn.Round("price", 1),
        "round_float": fn.Round("rating"),
        "mod": fn.Mod("pages", Value(3)),
        "power": fn.Power(Value(2), Value(10)),
        "sqrt": fn.Sqrt("pages"),
        "exp": fn.Exp(Value(1.0)),
        "ln": fn.Ln(Value(10.0)),
        "log": fn.Log(Value(2.0), Value(8.0)),
        "pi": fn.Pi(),
        "sin": fn.Sin(Value(1.0)),
        "cos": fn.Cos(Value(1.0)),
        "tan": fn.Tan(Value(1.0)),
        "cot": fn.Cot(Value(1.0)),
        "asin": fn.ASin(Value(0.5)),
        "acos": fn.ACos(Value(0.5)),
        "atan": fn.ATan(Value(0.5)),
        "atan2": fn.ATan2(Value(1.0), Value(2.0)),
        "degrees": fn.Degrees(Value(1.0)),
        "radians": fn.Radians(Value(180.0)),
        "sign": fn.Sign(Value(-3)),
        "greatest": fn.Greatest("pages", Value(5)),
        "least": fn.Least("pages", Value(5)),
        "nullif": fn.NullIf("pages", Value(10)),
        "coalesce": fn.Coalesce("reading_time", Value(datetime.timedelta(0))),
        "cast_int": fn.Cast("price", IntegerField()),
        "cast_char": fn.Cast("pages", CharField()),
        "cast_date": fn.Cast("published", DateField()),
        "cast_float": fn.Cast("price", FloatField()),
        "extract_year": fn.ExtractYear("published"),
        "extract_iso_year": fn.ExtractIsoYear("published"),
        "extract_quarter": fn.ExtractQuarter("published"),
        "extract_month": fn.ExtractMonth("published"),
        "extract_week": fn.ExtractWeek("published"),
        "extract_week_day": fn.ExtractWeekDay("published"),
        "extract_iso_week_day": fn.ExtractIsoWeekDay("published"),
        "extract_day": fn.ExtractDay("published"),
        "extract_hour": fn.ExtractHour("published"),
        "extract_minute": fn.ExtractMinute("published"),
        "extract_second": fn.ExtractSecond("published"),
        "extract_date_year": fn.ExtractYear("author__born"),
        "extract_duration_hour": fn.Extract("reading_time", "hour"),
        "trunc_year": fn.TruncYear("published"),
        "trunc_quarter": fn.TruncQuarter("published"),
        "trunc_month": fn.TruncMonth("published"),
        "trunc_week": fn.TruncWeek("published"),
        "trunc_day": fn.TruncDay("published"),
        "trunc_date": fn.TruncDate("published"),
        "trunc_time": fn.TruncTime("published"),
        "trunc_hour": fn.TruncHour("published"),
        "trunc_minute": fn.TruncMinute("published"),
        "trunc_second": fn.TruncSecond("published"),
        "trunc_date_month": fn.TruncMonth("author__born", output_field=DateField()),
        "datetime_plus_duration": ExpressionWrapper(F("published") + F("reading_time"),
                                                    output_field=DateTimeField()),
        "json_object": fn.JSONObject(title="title", pages="pages"),
        "json_array": fn.JSONArray("title", "pages"),
    }
    probes = {name: (lambda e=expression: books.filter(pk=book.pk).annotate(
        value=e).values_list("value", flat=True).get()) for name, expression
              in functions.items()}
    check_golden(GOLDENS / "functions.json", outcomes(probes))


def test_aggregates(migrated):
    from django.contrib.postgres import aggregates as pg
    from django.db.models import (AnyValue, Avg, BitAnd, BitOr, BitXor, Count, Max,
                                  Min, Q, StdDev, StringAgg, Sum, Value, Variance)

    _, books = library("Spectre")
    aggregates = {
        "count": Count("pk"),
        "count_distinct": Count("in_print", distinct=True),
        "count_filter": Count("pk", filter=Q(pages__gt=15)),
        "sum": Sum("pages"),
        "sum_filter": Sum("pages", filter=Q(in_print=True)),
        "avg": Avg("price"),
        "min": Min("published"),
        "max": Max("title"),
        "stddev_pop": StdDev("pages"),
        "stddev_samp": StdDev("pages", sample=True),
        "var_pop": Variance("pages"),
        "var_samp": Variance("pages", sample=True),
        "any_value": AnyValue("author__name"),
        "string_agg": StringAgg("title", Value(", "), order_by="title"),
        "array_agg": pg.ArrayAgg("pages", order_by="-pages"),
        "array_agg_distinct": pg.ArrayAgg("in_print", distinct=True, order_by="in_print"),
        "bool_and": pg.BoolAnd("in_print"),
        "bool_or": pg.BoolOr("in_print"),
        "bit_and": BitAnd("pages"),
        "bit_or": BitOr("pages"),
        "bit_xor": BitXor("pages"),
        "corr": pg.Corr("pages", "price"),
        "covar_pop": pg.CovarPop("pages", "price"),
        "covar_samp": pg.CovarPop("pages", "price", sample=True),
        "regr_avgx": pg.RegrAvgX("pages", "price"),
        "regr_avgy": pg.RegrAvgY("pages", "price"),
        "regr_count": pg.RegrCount("pages", "price"),
        "regr_intercept": pg.RegrIntercept("pages", "price"),
        "regr_r2": pg.RegrR2("pages", "price"),
        "regr_slope": pg.RegrSlope("pages", "price"),
        "regr_sxx": pg.RegrSXX("pages", "price"),
        "regr_sxy": pg.RegrSXY("pages", "price"),
        "regr_syy": pg.RegrSYY("pages", "price"),
        "jsonb_agg": pg.JSONBAgg("title", order_by="title"),
    }
    probes = {name: (lambda a=aggregate: books.aggregate(value=a)["value"])
              for name, aggregate in aggregates.items()}
    check_golden(GOLDENS / "aggregates.json", outcomes(probes))


def test_lookups(migrated):
    from django.db.models import Q

    writer, books = library("Weaver")
    utc = datetime.timezone.utc
    lookups = {
        "exact": Q(title="time walk"),
        "iexact": Q(title__iexact="TIME WALK"),
        "contains": Q(title__contains="sphere"),
        "icontains": Q(title__icontains="SPHERE"),
        "in": Q(pages__in=[10, 30, 99]),
        "gt": Q(pages__gt=30),
        "gte": Q(pages__gte=30),
        "lt": Q(pages__lt=30),
        "lte": Q(pages__lte=30),
        "startswith": Q(title__startswith="Chrono"),
        "istartswith": Q(title__istartswith="time"),
        "endswith": Q(title__endswith="20"),
        "iendswith": Q(title__iendswith="lock"),
        "range": Q(pages__range=(20, 40)),
        "isnull": Q(rating__isnull=True),
        "regex": Q(title__regex=r"^[A-Z]+$"),
        "iregex": Q(title__iregex=r"^time"),
        "date": Q(published__date=datetime.date(2020, 3, 15)),
        "date_gt": Q(published__date__gt=datetime.date(2020, 1, 1)),
        "year": Q(published__year=2020),
        "year_gte": Q(published__year__gte=2020),
        "iso_year": Q(published__iso_year=2021),
        "quarter": Q(published__quarter=1),
        "month": Q(published__month=12),
        "week": Q(published__week=52),
        "week_day": Q(published__week_day=4),
        "iso_week_day": Q(published__iso_week_day=3),
        "day": Q(published__day=15),
        "time": Q(published__time=datetime.time(7, 8, 9)),
        "hour": Q(published__hour=12),
        "minute": Q(published__minute=59),
        "second": Q(published__second=59),
        "datetime_range": Q(published__range=(
            datetime.datetime(2020, 1, 1, tzinfo=utc),
            datetime.datetime(2021, 1, 1, tzinfo=utc))),
        "duration_gt": Q(reading_time__gt=datetime.timedelta(hours=1)),
        "decimal_gte": Q(price__gte=decimal.Decimal("2.5")),
        "float_lt": Q(rating__lt=4.9),
        "boolean": Q(in_print=False),
        "binary_exact": Q(cover=b"time walk"),
        "date_of_fk": Q(author__born__year=1987),
        "fk_field": Q(author__name="Weaver"),
        "m2m_field": Q(tags__label__endswith="-horror"),
        "m2m_isnull": Q(tags__isnull=True),
        "negated": ~Q(title__startswith="Chrono"),
        "or": Q(pages=10) | Q(rating__isnull=True),
    }
    probes = {name: (lambda q=lookup: books.filter(q).count())
              for name, lookup in lookups.items()}
    check_golden(GOLDENS / "lookups.json", outcomes(probes))


def test_full_text_search(migrated):
    from django.contrib.postgres.search import (SearchHeadline, SearchQuery,
                                                SearchRank, SearchVector)

    _, books = library("Enigma")
    vector = SearchVector("title", config="english")
    searches = {
        "plain": SearchQuery("chronosphere", config="english"),
        "phrase": SearchQuery("time walk", search_type="phrase", config="english"),
        "raw": SearchQuery("time & !lock", search_type="raw", config="english"),
        "websearch": SearchQuery('"time walk" or backtrack', search_type="websearch",
                                 config="english"),
        "negated": ~SearchQuery("time", config="english"),
        "combined": SearchQuery("time", config="english")
        | SearchQuery("backtrack", config="english"),
    }
    probes = {f"match_{name}": (lambda q=query: sorted(books.annotate(
        search=vector).filter(search=q).values_list("pages", flat=True)))
              for name, query in searches.items()}
    probes["rank"] = lambda: list(books.annotate(rank=SearchRank(
        vector, searches["combined"])).filter(rank__gt=0).order_by(
        "-rank", "pages").values_list("pages", flat=True))
    probes["headline"] = lambda: books.annotate(headline=SearchHeadline(
        "title", searches["plain"], config="english")).values_list(
        "headline", flat=True).get(pages=10)
    probes["lookup"] = lambda: books.filter(title__search="walk").count()
    check_golden(GOLDENS / "search.json", outcomes(probes))


def admin_client(username: str):
    from django.contrib.auth.models import User
    from django.test import Client

    client = Client()
    client.force_login(User.objects.create_superuser(username, f"{username}@dota",
                                                     "x marks the spot"))
    return client


def test_admin_pages(migrated):
    from django.contrib.admin.models import ADDITION, LogEntry
    from django_drv.models import Book, Review

    writer, books = library("Tidehunter")
    client = admin_client("kunkka")
    book = Book.objects.create(author=writer, title="Anchor Smash", pages=12,
                               price=decimal.Decimal("9.99"), isbn="anchor")
    book.tags.add(books.get(pages=10).tags.get())
    Review.objects.create(book=book, stars=4)
    LogEntry.objects.log_actions(user_id=client.session["_auth_user_id"],
                                 queryset=[book], action_flag=ADDITION,
                                 change_message="Added.")
    pages = {
        "/admin/": "Site administration",
        "/admin/django_drv/book/": "Anchor Smash",
        "/admin/django_drv/book/?q=chrono": "Chronosphere 20",
        "/admin/django_drv/book/?in_print__exact=0&o=-3": "BACKTRACK",
        f"/admin/django_drv/book/?author__id__exact={writer.pk}&tags__isnull=False":
            "Chronosphere 10",
        "/admin/django_drv/book/?published__year=2020&published__month=3":
            "Chronosphere 20",
        "/admin/django_drv/author/?born__year=1987": "Tidehunter",
        f"/admin/django_drv/book/{book.pk}/change/": "Anchor Smash",
        f"/admin/django_drv/book/{book.pk}/history/": "Added",
        f"/admin/django_drv/book/{book.pk}/delete/": "Review",
        "/admin/autocomplete/?app_label=django_drv&model_name=book"
        "&field_name=author&term=Tide": "Tidehunter",
        "/admin/django_drv/book/add/": "Add book",
    }
    for url, text in pages.items():
        response = client.get(url)
        assert response.status_code == 200, url
        assert text in response.content.decode(), url


@pytest.mark.xfail(SERENEDB, strict=True,
                   reason="SereneDB has no SAVEPOINT (#722), the admin validates "
                          "CheckConstraints with Q.check() in a savepoint")
def test_admin_writes(migrated):
    from django_drv.models import Book

    writer, books = library("Kunkka")
    client = admin_client("tidebringer")
    form = {
        "uid": str(uuid.uuid4()), "author": writer.pk, "title": "Ghost Ship",
        "pages": 12, "price": "9.99", "rating": "4.0", "in_print": "on",
        "published_0": "2024-01-02", "published_1": "03:04:05",
        "reading_time": "01:00:00", "isbn": "ghost",
        "tags": [books.get(pages=10).tags.get().pk],
    }
    added = client.post("/admin/django_drv/book/add/", form)
    assert added.status_code == 302, added.content.decode()[-3000:]
    book = Book.objects.get(title="Ghost Ship")
    changed = client.post(f"/admin/django_drv/book/{book.pk}/change/",
                          form | {"title": "Ghost Ship II", "rating": "4.5"})
    assert changed.status_code == 302, changed.content.decode()[-3000:]
    assert Book.objects.get(pk=book.pk).title == "Ghost Ship II"


def test_server_side_binding(migrated):
    from django_drv.models import Author, Book

    writer = Author.objects.using("server_binding").create(
        name="Rubick", born=datetime.date(1999, 9, 9))
    Book.objects.using("server_binding").bulk_create([
        Book(author=writer, title=f"Spell Steal {i}", pages=i, price=decimal.Decimal(i),
             reading_time=datetime.timedelta(minutes=i), isbn=f"ss{i}")
        for i in range(1, 6)])
    books = Book.objects.using("server_binding").filter(author=writer)
    assert books.filter(pages__in=[1, 3, 9]).count() == 2
    assert books.filter(price__gte=decimal.Decimal("2.5"),
                        reading_time__lt=datetime.timedelta(minutes=5)).count() == 2
    assert books.filter(author__born=datetime.date(1999, 9, 9), rating=None).count() == 5
    assert books.get(uid=books.get(pages=4).uid).title == "Spell Steal 4"


def test_connection_pool(migrated):
    from django.db import connections
    from django_drv.models import Tag

    for i in range(5):
        Tag.objects.using("pooled").create(label=f"pool-{i}")
        connections["pooled"].close()
    assert Tag.objects.using("pooled").filter(label__startswith="pool-").count() == 5


def test_auth_and_sessions(migrated):
    from django.contrib.auth import authenticate
    from django.contrib.auth.models import Group, Permission, User
    from django.contrib.sessions.backends.db import SessionStore

    user = User.objects.create_superuser("rubick", "r@dota", "spell steal")
    assert authenticate(username="rubick", password="spell steal") == user
    group = Group.objects.create(name="carries")
    group.permissions.add(*Permission.objects.filter(codename__startswith="view_"))
    user.groups.add(group)
    assert user.has_perm("django_drv.view_book")
    session = SessionStore()
    session["hero"] = "rubick"
    session.create()
    assert SessionStore(session_key=session.session_key)["hero"] == "rubick"


def test_connection_features():
    from django.db import connection

    connection.ensure_connection()
    assert connection.pg_version >= 180000
    with connection.cursor() as cursor:
        cursor.execute("SHOW TIME ZONE")
        assert cursor.fetchone()[0] in ("UTC", "Etc/UTC")


@pytest.mark.xfail(SERENEDB, strict=True,
                   reason="SereneDB has no jsonb column type (#613)")
def test_migrate_postgres_fields(migrated):
    call("migrate", "django_drv_pg", interactive=False)
    from django_drv_pg.models import Document

    Document.objects.create(data={"hero": "pudge", "level": 25,
                                  "items": ["blink", "hook"]},
                            keywords=["meat", "hook"], scores=[1, 2, 3])
    assert Document.objects.filter(data__hero="pudge").count() == 1
    assert Document.objects.filter(data__level__gt=20).count() == 1
    assert Document.objects.filter(data__contains={"hero": "pudge"}).count() == 1
    assert Document.objects.filter(data__has_key="items").count() == 1
    assert Document.objects.filter(keywords__contains=["hook"]).count() == 1
    assert Document.objects.filter(keywords__overlap=["meat", "x"]).count() == 1
    assert Document.objects.filter(scores__len=3).count() == 1
    assert Document.objects.filter(scores__0=1).count() == 1


def test_unapply_migrations(migrated):
    call("migrate", "django_drv", "zero", interactive=False)
    from django.db import connection

    with connection.cursor() as cursor:
        assert not set(TABLES) & set(connection.introspection.table_names(cursor))
    call("migrate", "django_drv", interactive=False)
