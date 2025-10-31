web: gunicorn s2u_project.wsgi:application --bind 0.0.0.0:$PORT
release: bash -lc '/opt/venv/bin/python s2u_project/manage.py migrate --noinput && /opt/venv/bin/python s2u_project/manage.py sync_all_monthly_sales --days 30'
