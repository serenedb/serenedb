from __future__ import annotations

import os

import django
import psycopg
from django.conf import settings
from spec_loader import conn_kwargs

CORE_APPS = ["contenttypes", "auth", "sessions", "admin", "django_drv"]


def database_name() -> str:
    return f"drv_django_{os.environ.get('SDB_DRV_RUN_ID', '0')}"


def stock_database_name() -> str:
    return f"{database_name()}_stock"


def admin_connection() -> psycopg.Connection:
    return psycopg.connect(**conn_kwargs(), autocommit=True)


def database_settings(engine: str, dbname: str) -> dict:
    kw = conn_kwargs()
    return {
        "ENGINE": engine,
        "NAME": dbname,
        "USER": kw["user"],
        "PASSWORD": os.environ.get("SDB_DRV_PASSWORD", ""),
        "HOST": kw["host"],
        "PORT": str(kw["port"]),
    }


def configure() -> None:
    if settings.configured:
        return
    serenedb = database_settings("django_serenedb", database_name()) | {
        "DISABLE_SERVER_SIDE_CURSORS": True}
    settings.configure(
        DATABASES={
            "default": serenedb,
            "server_cursors": serenedb | {"DISABLE_SERVER_SIDE_CURSORS": False},
            "server_binding": serenedb | {"OPTIONS": {"server_side_binding": True}},
            "pooled": serenedb | {"OPTIONS": {"pool": True}},
            "stock": database_settings("django.db.backends.postgresql",
                                       stock_database_name()),
        },
        ROOT_URLCONF="django_drv.urls",
        ALLOWED_HOSTS=["testserver"],
        STATIC_URL="/static/",
        MIDDLEWARE=[
            "django.contrib.sessions.middleware.SessionMiddleware",
            "django.contrib.auth.middleware.AuthenticationMiddleware",
            "django.contrib.messages.middleware.MessageMiddleware",
        ],
        TEMPLATES=[{
            "BACKEND": "django.template.backends.django.DjangoTemplates",
            "APP_DIRS": True,
            "OPTIONS": {"context_processors": [
                "django.template.context_processors.request",
                "django.contrib.auth.context_processors.auth",
                "django.contrib.messages.context_processors.messages",
            ]},
        }],
        INSTALLED_APPS=[
            "django.contrib.contenttypes",
            "django.contrib.auth",
            "django.contrib.sessions",
            "django.contrib.messages",
            "django.contrib.admin",
            "django.contrib.postgres",
            "django_drv",
            "django_drv_pg",
        ],
        USE_TZ=True,
        TIME_ZONE="UTC",
        DEFAULT_AUTO_FIELD="django.db.models.BigAutoField",
        SECRET_KEY="drivers",
        CACHES={
            "default": {
                "BACKEND": "django.core.cache.backends.db.DatabaseCache",
                "LOCATION": "drv_cache",
            }
        },
    )
    django.setup()


def create_schema() -> None:
    from django.apps import apps
    from django.db import connection

    pending = [model for label in CORE_APPS
               for model in apps.get_app_config(label).get_models()]
    created = set()
    with connection.schema_editor() as editor:
        while pending:
            model = next(m for m in pending if references(m) <= created)
            editor.create_model(model)
            created.add(model)
            pending.remove(model)


def references(model) -> set:
    fields = model._meta.local_fields + model._meta.local_many_to_many
    return {field.remote_field.model for field in fields
            if field.remote_field and field.remote_field.model is not model}
