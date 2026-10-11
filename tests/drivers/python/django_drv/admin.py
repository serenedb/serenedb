from django.contrib import admin

from .models import Author, Book, Review, Tag


@admin.register(Author)
class AuthorAdmin(admin.ModelAdmin):
    list_display = ["name", "email", "born"]
    search_fields = ["name", "email"]
    date_hierarchy = "born"
    ordering = ["name"]


@admin.register(Book)
class BookAdmin(admin.ModelAdmin):
    list_display = ["title", "author", "pages", "price", "in_print", "published"]
    list_filter = ["in_print", "author", "tags", "published"]
    search_fields = ["title", "isbn", "author__name"]
    date_hierarchy = "published"
    autocomplete_fields = ["author"]
    filter_horizontal = ["tags"]


admin.site.register(Tag)
admin.site.register(Review)
