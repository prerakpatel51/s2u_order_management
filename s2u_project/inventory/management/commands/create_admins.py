from django.core.management.base import BaseCommand, CommandError
from django.contrib.auth import get_user_model
from django.utils.crypto import get_random_string


class Command(BaseCommand):
    help = "Create or promote usernames to admin (is_staff + is_superuser)."

    def add_arguments(self, parser):
        parser.add_argument("usernames", nargs="+", help="Usernames to create/promote")
        parser.add_argument(
            "--password",
            dest="password",
            help="Password to set for newly created users (or when --reset given)",
        )
        parser.add_argument(
            "--reset",
            action="store_true",
            help="Also reset password for existing users (requires --password)",
        )

    def handle(self, *args, **options):
        """Create or promote users to admin and optionally set/reset password.

        Example (CLI):
            # Promote existing users alice and bob
            python manage.py create_admins alice bob

            # Create user and set password
            python manage.py create_admins carol --password s3cret

            # Reset password for an existing admin
            python manage.py create_admins alice --password newpass --reset
        """
        User = get_user_model()
        pw = options.get("password")
        reset = options.get("reset")
        if reset and not pw:
            raise CommandError("--reset requires --password")

        for username in options["usernames"]:
            user, created = User.objects.get_or_create(username=username)
            changed = []
            if created:
                # If no password provided, generate one
                password = pw or get_random_string(12)
                user.set_password(password)
                changed.append("password")
            elif reset and pw:
                user.set_password(pw)
                changed.append("password_reset")

            # Promote to admin
            if not user.is_staff:
                user.is_staff = True
                changed.append("is_staff")
            if not user.is_superuser:
                user.is_superuser = True
                changed.append("is_superuser")
            if not user.is_active:
                user.is_active = True
                changed.append("is_active")

            user.save()

            if created and not pw:
                self.stdout.write(
                    self.style.WARNING(
                        f"Created admin '{username}' with temporary password shown below. Please change it on first login."
                    )
                )
                self.stdout.write(self.style.WARNING(f"username: {username}\npassword: {password}"))
            else:
                self.stdout.write(
                    self.style.SUCCESS(
                        f"{'Created' if created else 'Updated'} admin '{username}' ({', '.join(changed) or 'no changes'})"
                    )
                )
