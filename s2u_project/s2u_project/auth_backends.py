from django.contrib.auth.backends import ModelBackend
from django.contrib.auth import get_user_model


class CaseInsensitiveModelBackend(ModelBackend):
    """Authenticate with case-insensitive usernames.

    - Trims surrounding whitespace
    - Uses username__iexact for lookup
    """

    def authenticate(self, request, username=None, password=None, **kwargs):  # noqa: D401
        if username is None or password is None:
            return None
        username = str(username).strip()
        if not username:
            return None

        UserModel = get_user_model()
        try:
            user = UserModel.objects.get(username__iexact=username)
        except UserModel.DoesNotExist:
            return None
        else:
            if user.check_password(password) and self.user_can_authenticate(user):
                return user
        return None

