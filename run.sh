#!/bin/bash

# Run the application with gunicorn
gunicorn --bind 0.0.0.0:8000 --workers 4 --timeout 120 "src.main:create_app()"
