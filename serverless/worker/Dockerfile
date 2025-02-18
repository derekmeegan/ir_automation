# Use a Debian Bullseye based Python image
FROM python:3.9-slim-bullseye

# Set environment variable to disable Python buffering
ENV PYTHONUNBUFFERED=1

# Install system dependencies required by Chromium and Lambda Runtime Interface Client
RUN apt-get update && apt-get install -y \
    wget \
    curl \
    libnss3 \
    libatk1.0-0 \
    libatk-bridge2.0-0 \
    libcups2 \
    libxkbcommon0 \
    libxcomposite1 \
    libxdamage1 \
    libxi6 \
    libxtst6 \
    libgbm1 \
    libasound2 \
    && apt-get clean && rm -rf /var/lib/apt/lists/*

# Install AWS Lambda Runtime Interface Client
RUN pip install awslambdaric

# Set working directory
WORKDIR /var/task

# Copy and install Python dependencies
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

# Install Playwright and Chromium (which now works with Bullseye's glibc)
RUN playwright install chromium

# Copy the rest of the application code
COPY . .

# Use the AWS Lambda Runtime Interface Client as the entrypoint
CMD [ "awslambdaric", "handler.lambda_handler" ]