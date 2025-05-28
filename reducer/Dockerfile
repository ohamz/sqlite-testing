FROM python:3.10-slim

# Install shell utilities and dependencies
RUN apt-get update && apt-get install -y bash coreutils diffutils dos2unix

# Install sqlglot
RUN pip install sqlglot

# Create working directory
WORKDIR /app

# Copy scripts and source files
COPY reducer main.py test.sh original_test.sql test.sql ./
COPY src/ ./src/

# Convert line endings and make scripts executable
RUN dos2unix reducer test.sh && chmod +x reducer test.sh

# Default command (can be overridden)
CMD ["./reducer", "--query", "original_test.sql", "--test", "./test.sh", "--mode", "diff"]
