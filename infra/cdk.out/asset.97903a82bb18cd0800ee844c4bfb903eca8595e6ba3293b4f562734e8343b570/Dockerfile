# Use a lightweight Python base image
FROM public.ecr.aws/lambda/python:3.9

# Set environment variables to avoid Python buffering
ENV PYTHONUNBUFFERED=1

# Install system dependencies
RUN apt-get update && apt-get install -y \
    wget \
    curl \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Create a working directory
WORKDIR /app

# Copy only requirements to leverage Docker caching
COPY requirements.txt /app/

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

RUN playwright install --with-deps chromium

# Copy the rest of the application code
COPY . .

# Run your script as the container's entrypoint
CMD [ "handler.lambda_handler" ]
