web: gunicorn s2u_project.wsgi:application --bind 0.0.0.0:$PORT --timeout 3600 --graceful-timeout 30 --keep-alive 5
release: bash -lc '/opt/venv/bin/python s2u_project/manage.py migrate --noinput && /opt/venv/bin/python s2u_project/manage.py sync_all_monthly_sales --days 30'
