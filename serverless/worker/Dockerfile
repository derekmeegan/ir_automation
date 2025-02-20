# Define the function directory as an argument (default: /var/task)
ARG FUNCTION_DIR="/var/task"

##############################
# Build Stage
##############################
FROM python:3.9-slim-bullseye AS build-stage

ARG FUNCTION_DIR
WORKDIR ${FUNCTION_DIR}

# Copy all application code and requirements
COPY . ${FUNCTION_DIR}

# Install system dependencies (required for Chromium and the runtime)
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
    libxfixes3 \
    libxrandr2 \
    libpango-1.0-0 \
    libcairo2 \
    && apt-get clean && rm -rf /var/lib/apt/lists/*

# Install AWS Lambda Runtime Interface Client into the target directory
RUN pip install --target ${FUNCTION_DIR} awslambdaric

# Install Python dependencies into the target directory
RUN pip install --no-cache-dir --target ${FUNCTION_DIR} -r requirements.txt

# Install Playwright and Chromium
RUN python -m playwright install chromium

##############################
# Final Stage
##############################
FROM python:3.9-slim-bullseye

ENV PYTHONUNBUFFERED=1

# Install system dependencies required at runtime
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
    libxfixes3 \
    libxrandr2 \
    libpango-1.0-0 \
    libcairo2 \
    && apt-get clean && rm -rf /var/lib/apt/lists/*

ARG FUNCTION_DIR="/var/task"
WORKDIR ${FUNCTION_DIR}

# Copy the built application files from the build stage
COPY --from=build-stage ${FUNCTION_DIR} ${FUNCTION_DIR}

# Set the runtime interface client as the entrypoint
ENTRYPOINT [ "/usr/local/bin/python", "-m", "awslambdaric" ]
# Pass the Lambda handler (adjust if your module name or handler changes)
CMD [ "handler.lambda_handler" ]