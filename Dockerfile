FROM python:3.9-slim

# Set working directory to /app
WORKDIR /app

# Install build dependencies
RUN apt-get update && apt-get install -y build-essential libssl-dev libffi-dev

# Copy requirements file
COPY requirements.txt .

# Install dependencies
RUN pip install -r requirements.txt

# Copy application code
COPY . .

# Run command when container starts
CMD ["python", "aisstreamio_pipeline.py"]