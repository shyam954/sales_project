# Use official Python image
FROM python:3.9-slim

# Set working directory
WORKDIR /app

# Copy project files
COPY . /app

# Upgrade pip and install pipenv
RUN pip install --upgrade pip
RUN pip install pipenv

# Install dependencies in the container
RUN pipenv install --system --deploy

# Run the main script
CMD ["python", "main.py"]
