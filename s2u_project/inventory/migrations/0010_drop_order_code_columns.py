from django.db import migrations


class Migration(migrations.Migration):

    dependencies = [
        ("inventory", "0009_productbarcode"),
    ]

    operations = [
        # Drop leftover columns from a reverted change; guard with IF EXISTS so it's safe.
        migrations.RunSQL(
            sql=(
                "ALTER TABLE inventory_weeklyorderitem DROP COLUMN IF EXISTS order_code;\n"
                "ALTER TABLE inventory_product DROP COLUMN IF EXISTS order_code;\n"
            ),
            reverse_sql=migrations.RunSQL.noop,
        ),
    ]

