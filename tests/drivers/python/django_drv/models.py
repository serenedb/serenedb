import uuid

from django.db import models


class Author(models.Model):
    name = models.CharField(max_length=100, unique=True)
    email = models.EmailField(blank=True)
    bio = models.TextField(null=True, db_comment="free text about the author")
    born = models.DateField(null=True)

    class Meta:
        db_table_comment = "people who write books"
        indexes = [models.Index(fields=["born"], name="drv_author_born_idx")]

    def __str__(self):
        return self.name


class Tag(models.Model):
    label = models.SlugField(unique=True)

    def __str__(self):
        return self.label


class Book(models.Model):
    uid = models.UUIDField(default=uuid.uuid4, unique=True)
    author = models.ForeignKey(Author, on_delete=models.CASCADE,
                               related_name="books")
    title = models.CharField(max_length=200)
    pages = models.PositiveIntegerField(default=0)
    price = models.DecimalField(max_digits=8, decimal_places=2)
    rating = models.FloatField(null=True)
    in_print = models.BooleanField(default=True)
    published = models.DateTimeField(null=True)
    reading_time = models.DurationField(null=True)
    cover = models.BinaryField(null=True)
    isbn = models.CharField(max_length=13, db_index=True)
    tags = models.ManyToManyField(Tag, related_name="books")

    class Meta:
        constraints = [
            models.UniqueConstraint(fields=["author", "title"],
                                    name="drv_book_author_title_uniq"),
            models.CheckConstraint(condition=models.Q(price__gte=0),
                                   name="drv_book_price_nonneg"),
        ]

    def __str__(self):
        return self.title


class Review(models.Model):
    book = models.OneToOneField(Book, on_delete=models.CASCADE)
    stars = models.SmallIntegerField()
    created = models.DateTimeField(auto_now_add=True)
    reviewer_ip = models.GenericIPAddressField(null=True)
    likes = models.BigIntegerField(default=0)
    opened_at = models.TimeField(null=True)
