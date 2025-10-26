from django.db import migrations, models
import django.db.models.deletion


class Migration(migrations.Migration):

    dependencies = [
        ("inventory", "0008_monthlysales"),
    ]

    operations = [
        migrations.CreateModel(
            name="ProductBarcode",
            fields=[
                ("id", models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                ("code", models.CharField(max_length=64)),
                (
                    "product",
                    models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, related_name="barcodes", to="inventory.product"),
                ),
            ],
            options={
                "unique_together": {("product", "code")},
            },
        ),
        migrations.AddIndex(
            model_name="productbarcode",
            index=models.Index(fields=["code"], name="inventory_p_code_idx"),
        ),
    ]

