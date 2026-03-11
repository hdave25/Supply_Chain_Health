# Use a slim Python image to keep the size down
FROM python:3.11-slim

# Set the working directory inside the container
WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# Copy and install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the rest of your application code
COPY . .

# Ensure the entrypoint script is executable
RUN chmod +x entrypoint.sh

# Expose the port FastAPI runs on
EXPOSE 8000

# Use the script to run your pipelines then start uvicorn
ENTRYPOINT ["./entrypoint.sh"]