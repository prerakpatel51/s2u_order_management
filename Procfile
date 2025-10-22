web: gunicorn s2u_project.wsgi:application --bind 0.0.0.0:$PORT
release: python manage.py migrate
