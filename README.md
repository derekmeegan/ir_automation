# IR Automation

A serverless application for automating the extraction and analysis of financial information from investor relations sites. This system monitors for earnings releases, extracts key financial metrics, and delivers insights via Discord notifications.

## Overview

IR Automation is a Python-based serverless application that:

1. Monitors upcoming earnings announcements
2. Automatically scrapes investor relations websites for earnings releases
3. Extracts key financial metrics using AI (Groq)
4. Analyzes the data and generates insights
5. Delivers reports via Discord notifications

## Architecture

The application is built on AWS serverless architecture using AWS CDK for infrastructure as code:

- **Scheduler**: Lambda function that monitors upcoming earnings dates
- **Worker**: Container-based service that processes earnings releases
- **Database**: DynamoDB tables for storing data and tracking processing status
- **Manager**: API Gateway and Lambda functions for manual control

### Components

- `services/worker`: Processes earnings releases using Playwright and Groq LLM
- `serverless/scheduler`: Monitors upcoming earnings announcements using Yahoo Finance
- `serverless/database_handlers`: Manages data persistence in DynamoDB
- `serverless/manager`: Provides API endpoints for manual control
- `infra`: AWS CDK infrastructure code
- `tests`: Unit and integration tests

## Setup and Deployment

### Prerequisites

- AWS Account
- AWS CLI configured with appropriate credentials
- Python 3.9+
- Node.js and npm (for AWS CDK)
- Groq API key
- Discord webhook URL (for notifications)

### Environment Variables

Create a `.env` file with the following variables:

```
GROQ_API_KEY=your_groq_api_key
DISCORD_WEBHOOK_URL=your_discord_webhook_url
```

### AWS Secrets

The following secrets should be stored in AWS Secrets Manager:

- `IRWorkflow/GroqApiKey`: Groq API key
- `IRWorkflow/DiscordWebhook`: Discord webhook URL

### Deployment

The application is deployed using GitHub Actions:

1. Push to the main branch triggers the deployment workflow
2. AWS CDK deploys the infrastructure to AWS

Manual deployment:

```bash
# Install dependencies
pip install -r infra/requirements.txt
npm install -g aws-cdk

# Deploy
cd infra
cdk deploy
```

## Local Testing

For local testing, use the `local_test.py` script:

```bash
python local_test.py
```

This script simulates the processing of an earnings release without deploying to AWS.

## Running Tests

The project uses pytest for testing. To run tests:

```bash
pytest
```

You can also use pre-commit hooks to run tests automatically before committing:

```bash
pre-commit install
```

## Configuration

The application is configured using a JSON configuration object that specifies:

- Base URL of the investor relations site
- Link templates for finding earnings releases
- CSS selectors for scraping
- Keywords for verifying correct earnings releases
- LLM instructions for extracting financial metrics

Example configuration is available in `local_test.py`.
