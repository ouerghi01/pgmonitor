# Use the official Python 3.11 slim image as a base
FROM python:3.11-slim
EXPOSE 8050


# Set the working directory in the container
WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

COPY . .

# Install Python dependencies
RUN python3 -m pip install --upgrade pip
RUN pip install -r requirements.txt
RUN pip install docker==7.1.0
RUN pip install  python-dotenv
# Download NLTK data
RUN python -m nltk.downloader vader_lexicon

# Command to run the application
CMD [ "python3", "main.py" ]
