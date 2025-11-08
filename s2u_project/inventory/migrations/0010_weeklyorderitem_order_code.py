from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("inventory", "0009_productbarcode"),
    ]

    operations = [
        migrations.AddField(
            model_name="weeklyorderitem",
            name="order_code",
            field=models.CharField(blank=True, max_length=64),
        ),
    ]

