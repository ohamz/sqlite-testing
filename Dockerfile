# Use Python base image
FROM python:3.11-slim

# Install docker CLI
RUN apt-get update && \
    apt-get install -y docker.io curl && \
    apt-get clean

# Install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy app code
COPY . /app
WORKDIR /app

# You can run this interactively or let CMD run it automatically
CMD ["python", "main.py"]
