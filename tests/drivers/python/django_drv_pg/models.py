from django.contrib.postgres.fields import ArrayField
from django.db import models


class Document(models.Model):
    data = models.JSONField(default=dict)
    keywords = ArrayField(models.CharField(max_length=30), default=list)
    scores = ArrayField(models.IntegerField(), null=True)
