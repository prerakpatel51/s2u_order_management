# Generated manually for renaming weekly_needed to monthly_needed
from django.db import migrations


class Migration(migrations.Migration):

    dependencies = [
        ('inventory', '0006_add_performance_indexes'),
    ]

    operations = [
        migrations.RenameField(
            model_name='weeklyorderitem',
            old_name='weekly_needed',
            new_name='monthly_needed',
        ),
    ]
