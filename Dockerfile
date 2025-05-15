FROM python:3.11-slim

# Install Docker CLI
RUN apt-get update && \
    apt-get install -y docker.io && \
    apt-get clean

# Install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy app code
COPY . /app
WORKDIR /app

# Copy and install executable script
COPY test-db /usr/bin/test-db
RUN chmod +x /usr/bin/test-db

# Default command
CMD ["/usr/bin/test-db"]
