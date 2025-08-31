# Use the official Python image
FROM python:3.9-slim

# Set the working directory
WORKDIR /app

# Copy the requirements file and install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the rest of the application code
COPY . .

# Set the entrypoint for the container
# Gunicorn is a production-ready web server
CMD ["gunicorn", "--bind", "0.0.0.0:8080", "app:app"]
