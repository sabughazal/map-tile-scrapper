#!/bin/bash

# Run the FastAPI application with uvicorn
uvicorn src.main:create_app --factory --host 0.0.0.0 --port 8000 --workers 64
