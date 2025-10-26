web: gunicorn s2u_project.wsgi:application --bind 0.0.0.0:$PORT
release: /opt/venv/bin/python s2u_project/manage.py migrate --noinput
