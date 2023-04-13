#!/bin/bash
# python manage.py makemigrations
# python manage.py migrate
# python manage.py init -y
gunicorn -c gunicorn.py application.asgi:application
