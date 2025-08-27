# Use Python 3.9 slim image
FROM python:3.9-slim

# Set working directory
WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    gcc \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements first for better caching
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY . .

# Create necessary directories
RUN mkdir -p data/logs data/uploads reports

# Set environment variables
ENV PYTHONPATH="/app/src:/app"
ENV FLASK_APP=web/app.py
ENV FLASK_ENV=production

# Expose port
EXPOSE 9000

# Create startup script
RUN echo '#!/bin/bash\n\
if [ "$1" = "web" ]; then\n\
    cd /app && python web/app.py\n\
elif [ "$1" = "analyze" ]; then\n\
    cd /app && python app.py "$@"\n\
else\n\
    cd /app && python web/app.py\n\
fi' > /app/entrypoint.sh && chmod +x /app/entrypoint.sh

# Default command - start web interface
CMD ["/app/entrypoint.sh", "web"]
