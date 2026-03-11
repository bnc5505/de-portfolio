# Start from an official Python image
FROM python:3.11-slim

# Set working directory inside the container
WORKDIR /app

# Copy and install dependencies first (better caching)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the ingestion script
COPY load_stock_data.py .

# Run the script when the container starts
CMD ["python", "load_stock_data.py"]