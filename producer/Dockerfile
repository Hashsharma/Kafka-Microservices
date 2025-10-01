# Base image for Python applications
FROM python:3.11-slim

# Set the working directory inside the container
WORKDIR /app

# Copy the requirements file and install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the application code
COPY . .

# Expose the port (FastAPI default)
EXPOSE 8000

# The command to run the application is defined in docker-compose.yml
# CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]
