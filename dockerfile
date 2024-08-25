# Use the official Python 3.11 slim image as a base
FROM python:3.11-slim

# Set the working directory in the container
WORKDIR /app
Copy . /app
# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*
RUN python3 -m pip install "pg_activity[psycopg]"
COPY ProducerConsumer/pg_activity.sh /scripts/pg_activity.sh
RUN chmod +x /scripts/pg_activity.sh
    
COPY requirements.txt .
RUN pip install -r requirements.txt
RUN pip install docker==7.1.0

# Download NLTK data
RUN python -m nltk.downloader vader_lexicon

# Copy the application files to the working directory

# Command to run the application
CMD [ "python3", "main.py" ]
