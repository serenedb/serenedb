# This is an auto-generated Django model module.
# You'll have to do the following manually to clean this up:
#   * Rearrange models' order
#   * Make sure each model has one field with primary_key=True
#   * Make sure each ForeignKey and OneToOneField has `on_delete` set to the desired behavior
#   * Remove `managed = False` lines if you wish to allow Django to create, modify, and delete the table
# Feel free to rename the models, but don't rename db_table values or field names.
from django.db import models


class DjangoDrvAuthor(models.Model):
    id = models.BigAutoField(primary_key=True)
    name = models.CharField(unique=True, max_length=100)
    email = models.CharField(max_length=254)
    bio = models.TextField(blank=True, null=True, db_comment='free text about the author')
    born = models.DateField(blank=True, null=True)

    class Meta:
        managed = False
        db_table = 'django_drv_author'
        db_table_comment = 'people who write books'


class DjangoDrvBook(models.Model):
    id = models.BigAutoField(primary_key=True)
    uid = models.UUIDField(unique=True)
    author = models.ForeignKey(DjangoDrvAuthor, models.DO_NOTHING)
    title = models.CharField(max_length=200)
    pages = models.IntegerField()
    price = models.DecimalField(max_digits=8, decimal_places=2)
    rating = models.FloatField(blank=True, null=True)
    in_print = models.BooleanField()
    published = models.DateTimeField(blank=True, null=True)
    reading_time = models.DurationField(blank=True, null=True)
    cover = models.BinaryField(blank=True, null=True)
    isbn = models.CharField(max_length=13)

    class Meta:
        managed = False
        db_table = 'django_drv_book'
        unique_together = (('author', 'title'),)


class DjangoDrvBookTags(models.Model):
    id = models.BigAutoField(primary_key=True)
    book = models.ForeignKey(DjangoDrvBook, models.DO_NOTHING)
    tag = models.ForeignKey('DjangoDrvTag', models.DO_NOTHING)

    class Meta:
        managed = False
        db_table = 'django_drv_book_tags'
        unique_together = (('book', 'tag'),)


class DjangoDrvReview(models.Model):
    id = models.BigAutoField(primary_key=True)
    book = models.OneToOneField(DjangoDrvBook, models.DO_NOTHING)
    stars = models.SmallIntegerField()
    created = models.DateTimeField()
    reviewer_ip = models.GenericIPAddressField(blank=True, null=True)
    likes = models.BigIntegerField()
    opened_at = models.TimeField(blank=True, null=True)

    class Meta:
        managed = False
        db_table = 'django_drv_review'


class DjangoDrvTag(models.Model):
    id = models.BigAutoField(primary_key=True)
    label = models.CharField(unique=True, max_length=50)

    class Meta:
        managed = False
        db_table = 'django_drv_tag'
