# Define the function directory as an argument (default: /var/task)
ARG FUNCTION_DIR="/var/task"

##############################
# Build Stage
##############################
FROM python:3.9-slim-bullseye AS build-stage

ARG FUNCTION_DIR
WORKDIR ${FUNCTION_DIR}

# Set environment variable for Playwright in build stage
ENV PLAYWRIGHT_BROWSERS_PATH=0

# Copy all application code and requirements
COPY . ${FUNCTION_DIR}

# Install system dependencies (required for Chromium, Firefox, and runtime)
RUN apt-get update && apt-get install -y --no-install-recommends \
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
    libx11-xcb1 \
    libxcursor1 \
    libgtk-3-0 \
    libpangocairo-1.0-0 \
    libcairo-gobject2 \
    libgdk-pixbuf-2.0-0 \
    && apt-get clean && rm -rf /var/lib/apt/lists/*

# Install Python dependencies into the target directory
RUN pip install --no-cache-dir --target ${FUNCTION_DIR} -r requirements.txt

# Install Playwright and Chromium and Firefox
RUN python -m playwright install chromium
RUN python -m playwright install firefox

##############################
# Final Stage
##############################
FROM python:3.9-slim-bullseye

ENV PYTHONUNBUFFERED=1
ENV PLAYWRIGHT_BROWSERS_PATH=0

# Install system dependencies required at runtime
RUN apt-get update && apt-get install -y --no-install-recommends \
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

# (Optional) Create a non-root user for enhanced security.
RUN adduser --disabled-password --gecos "" appuser && chown -R appuser ${FUNCTION_DIR}
USER appuser

EXPOSE 8080
# Set the container's entrypoint.
# Adjust the command if your application's main script is different.
CMD [ "python", "handler.py" ]
