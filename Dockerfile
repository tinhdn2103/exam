FROM python:3.9-slim

# Set environment variables
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

WORKDIR /app

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application source code
COPY . .

# Create results directory
RUN mkdir -p /app/results

# Default entrypoint
ENTRYPOINT ["python", "aggregator.py"]

# Default arguments (override with docker run)
# Example: docker run -v /path/to/data:/data ad-aggregator --input /data/ad_data.csv
CMD ["--input", "/data/ad_data.csv", "--output", "results/"]
