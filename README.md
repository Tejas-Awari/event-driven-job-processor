# Event-Driven Background Job Processor

A Node.js project that runs time-consuming tasks in the background using RabbitMQ. PostgreSQL keeps track of each job, and WebSockets notify clients when a job finishes or fails.

## What it does

- Creates jobs through a REST API
- Stores job details and status in PostgreSQL
- Uses RabbitMQ to send jobs to a background worker
- Simulates a long-running task with a 10-second delay
- Retries failed jobs up to three times
- Moves jobs that fail all attempts to a dead-letter queue
- Lets clients check job status through an API endpoint
- Sends completion and failure events over WebSockets
- Runs PostgreSQL and RabbitMQ locally with Docker Compose
- Includes an end-to-end test and a basic load test

## How it works

1. A client sends a request to create a job.
2. The API saves the job in PostgreSQL.
3. The job ID is published to RabbitMQ.
4. A worker picks up the job and processes it in the background.
5. The worker updates the job status and saves the result.
6. The API receives job events from RabbitMQ and forwards them to subscribed WebSocket clients.

## Tech Stack

- Node.js and Express
- PostgreSQL
- RabbitMQ and amqplib
- WebSockets (`ws`)
- PostgreSQL client (`pg`)
- Docker Compose
- Git and GitHub

## Getting started

### Requirements

Make sure Node.js 18 or newer and Docker Desktop are installed.

### Setup

Clone the repository and move into the project directory.

Copy `.env.example` to `.env` and fill in the local database and RabbitMQ settings.

Start the containers:

```bash
docker compose up -d
```

Install the project dependencies:

```bash
npm install
```

Start the API in one terminal:

```bash
npm run dev
```

Start the worker in another terminal:

```bash
npm run worker
```

## API

### `GET /health`

Checks whether the API is running.

### `POST /jobs`

Creates a new background job and adds it to the queue.

Example request body:

```json
{
  "type": "demo",
  "message": "hello"
}
```

### `GET /jobs/:id`

Returns the saved details for a job, including its current status, payload, result, attempt count, and error message.

## WebSocket notifications

Connect to:

```text
ws://localhost:3000
```

Subscribe to a specific job by sending:

```json
{
  "action": "subscribe",
  "jobId": "YOUR_JOB_ID"
}
```

The server can send events such as `job.completed`, `job.failed`, and `job.status`.

WebSocket notifications are not guaranteed to reach every client. Use `GET /jobs/:id` to check the latest saved job status.

## Retries and failed jobs

- Each job gets a maximum of three processing attempts.
- Failed jobs are retried after a five-second delay.
- Jobs that fail all three attempts are marked `FAILED` and sent to `job_dead_letter_queue`.

## Testing

Run the test suite:

```bash
npm test
```

Run the concurrent request test:

```bash
npm run loadtest
```

To submit 10 jobs during the load test:

```bash
JOBS=10 npm run loadtest
```

The load test checks job-submission responses. It is not a benchmark of maximum throughput or job-completion time.

## Current limitations

- Intended for local development and demonstration.
- No authentication, authorization, or TLS setup.
- The worker processes one job at a time.
- PostgreSQL updates and RabbitMQ publishing are not part of a single atomic transaction.
- WebSocket events can be missed if a client disconnects.
- High availability and production deployment are not configured yet.

