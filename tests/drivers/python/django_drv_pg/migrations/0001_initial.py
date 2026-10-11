
import django.contrib.postgres.fields
from django.db import migrations, models


class Migration(migrations.Migration):

    initial = True

    dependencies = [
    ]

    operations = [
        migrations.CreateModel(
            name='Document',
            fields=[
                ('id', models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('data', models.JSONField(default=dict)),
                ('keywords', django.contrib.postgres.fields.ArrayField(base_field=models.CharField(max_length=30), default=list)),
                ('scores', django.contrib.postgres.fields.ArrayField(base_field=models.IntegerField(), null=True)),
            ],
        ),
    ]
