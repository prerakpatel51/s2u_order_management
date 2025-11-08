from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("inventory", "0010_weeklyorderitem_order_code"),
    ]

    operations = [
        migrations.AddField(
            model_name="product",
            name="order_code",
            field=models.CharField(max_length=64, blank=True),
        ),
    ]

