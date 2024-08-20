# Use the official Python 3.11 slim image as a base
FROM python:3.11-slim

# Set the working directory in the container
WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

# Copy the application files to the working directory
COPY . /app

# Install Python dependencies
RUN pip install -r requirements.txt

# Command to run the application
CMD [ "python3", "main.py" ]
