
import django.db.models.deletion
import uuid
from django.db import migrations, models


class Migration(migrations.Migration):

    initial = True

    dependencies = [
    ]

    operations = [
        migrations.CreateModel(
            name='Tag',
            fields=[
                ('id', models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('label', models.SlugField(unique=True)),
            ],
        ),
        migrations.CreateModel(
            name='Author',
            fields=[
                ('id', models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('name', models.CharField(max_length=100, unique=True)),
                ('email', models.EmailField(blank=True, max_length=254)),
                ('bio', models.TextField(db_comment='free text about the author', null=True)),
                ('born', models.DateField(null=True)),
            ],
            options={
                'db_table_comment': 'people who write books',
                'indexes': [models.Index(fields=['born'], name='drv_author_born_idx')],
            },
        ),
        migrations.CreateModel(
            name='Book',
            fields=[
                ('id', models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('uid', models.UUIDField(default=uuid.uuid4, unique=True)),
                ('title', models.CharField(max_length=200)),
                ('pages', models.PositiveIntegerField(default=0)),
                ('price', models.DecimalField(decimal_places=2, max_digits=8)),
                ('rating', models.FloatField(null=True)),
                ('in_print', models.BooleanField(default=True)),
                ('published', models.DateTimeField(null=True)),
                ('reading_time', models.DurationField(null=True)),
                ('cover', models.BinaryField(null=True)),
                ('isbn', models.CharField(db_index=True, max_length=13)),
                ('author', models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, related_name='books', to='django_drv.author')),
                ('tags', models.ManyToManyField(related_name='books', to='django_drv.tag')),
            ],
        ),
        migrations.CreateModel(
            name='Review',
            fields=[
                ('id', models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('stars', models.SmallIntegerField()),
                ('created', models.DateTimeField(auto_now_add=True)),
                ('reviewer_ip', models.GenericIPAddressField(null=True)),
                ('likes', models.BigIntegerField(default=0)),
                ('opened_at', models.TimeField(null=True)),
                ('book', models.OneToOneField(on_delete=django.db.models.deletion.CASCADE, to='django_drv.book')),
            ],
        ),
        migrations.AddConstraint(
            model_name='book',
            constraint=models.UniqueConstraint(fields=('author', 'title'), name='drv_book_author_title_uniq'),
        ),
        migrations.AddConstraint(
            model_name='book',
            constraint=models.CheckConstraint(condition=models.Q(('price__gte', 0)), name='drv_book_price_nonneg'),
        ),
    ]
