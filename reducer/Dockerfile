FROM python:3.10-slim

# Install shell utilities and dependencies
RUN apt-get update && apt-get install -y bash coreutils diffutils dos2unix

# Install sqlglot
RUN pip install sqlglot

# Create working directory
WORKDIR /app

# Copy scripts and source files
COPY main.py test-diff.sh test-crash.sh query.sql ./
COPY src/ ./src/
COPY reducer /usr/bin/

# Convert line endings and make scripts executable
RUN dos2unix /usr/bin/reducer test-diff.sh && chmod +x /usr/bin/reducer test-diff.sh
RUN dos2unix /usr/bin/reducer test-crash.sh && chmod +x /usr/bin/reducer test-crash.sh

# Default command (can be overridden)
CMD ["/usr/bin/reducer", "--query", "query.sql", "--test", "./test-diff.sh"]
