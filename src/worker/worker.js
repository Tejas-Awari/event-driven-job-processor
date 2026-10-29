require("dotenv").config();
const amqp = require("amqplib");
const pool = require("../db/pool");
const JOB_EVENTS_QUEUE = "job_events";
const JOB_QUEUE = "job_queue";
const RETRY_QUEUE = "job_retry_queue";
const DLQ = "job_dead_letter_queue";
const MAX_ATTEMPTS = 3;
const RETRY_DELAY_MS = 5000;
function sleep(milliseconds) {
  return new Promise((resolve) => {
    setTimeout(resolve, milliseconds);
  });
}
async function publishConfirmed(channel, queue, content, options) {
  channel.sendToQueue(queue, content, options);
  await channel.waitForConfirms();
}

async function publishJobEvent(channel, event) {
  try {
    await publishConfirmed(
      channel,
      JOB_EVENTS_QUEUE,
      Buffer.from(JSON.stringify(event)),
      {
        persistent: true,
        contentType: "application/json"
      }
    );
  } catch (error) {
    console.error(
      `[Worker] Failed to publish ${event.event}:`,
      error.message
    );
  }
}
async function processJob(channel, message) {
  if (!message) {
    return;
  }
  let jobId;
  let retryCount = 0;
  try {
    const received = JSON.parse(message.content.toString());
    jobId = received.jobId;
    if (typeof jobId !== "string" || !jobId) {
      throw new Error("Message has no valid jobId");
    }
    const headerValue =
      message.properties.headers?.["x-retry-count"] ?? 0;
    retryCount = Number(headerValue);
    if (!Number.isInteger(retryCount) || retryCount < 0) {
      throw new Error("Invalid retry-count header");
    }
  } catch (error) {
    console.error("[Worker] Invalid message:", error.message);
    try {
      await publishConfirmed(
        channel,
        DLQ,
        Buffer.from(JSON.stringify({
          reason: error.message,
          originalMessage: message.content.toString(),
          failedAt: new Date().toISOString()
        })),
        {
          persistent: true,
          contentType: "application/json"
        }
      );
      channel.ack(message);
    } catch (publishError) {
      console.error("[Worker] Could not send invalid message to DLQ:",
        publishError.message);
      channel.nack(message, false, true);
    }
    return;
  }
  const attemptNumber = retryCount + 1;
  try {
    const started = await pool.query(
      `UPDATE jobs
       SET status = 'PROCESSING',
           attempts = attempts + 1,
           error_message = NULL,
           updated_at = CURRENT_TIMESTAMP
       WHERE id = $1
         AND status NOT IN ('COMPLETED', 'FAILED')
       RETURNING id, payload, attempts`,
      [jobId]
    );
    if (started.rowCount === 0) {
      console.log(
        `[Worker] Job not found or already finished: ${jobId}`
      );
      channel.ack(message);
      return;
    }
    const job = started.rows[0];
    console.log(
      `[Worker] Processing ${jobId}, attempt ${attemptNumber}/${MAX_ATTEMPTS}`
    );
    if (job.payload?.failAlways === true) {
      throw new Error("Simulated failure: failAlways is true");
    }
    await sleep(10000);
    const resultText = "Processing completed successfully";
    await pool.query(
      `UPDATE jobs
       SET status = 'COMPLETED',
           result = $2,
           error_message = NULL,
           updated_at = CURRENT_TIMESTAMP
       WHERE id = $1`,
      [jobId, "Processing completed successfully"]
    );
    await publishJobEvent(channel, {
      event: "job.completed",
      jobId,
      status: "COMPLETED",
      result: resultText,
      occurredAt: new Date().toISOString()
    });
    console.log(`[Worker] Completed job: ${jobId}`);
    channel.ack(message);
  } catch (error) {
    const errorMessage = String(error.message).slice(0, 2000);
    console.error(
      `[Worker] Attempt ${attemptNumber} failed for ${jobId}:`,
      errorMessage
    );
    try {
      if (attemptNumber < MAX_ATTEMPTS) {
        await pool.query(
          `UPDATE jobs
           SET status = 'QUEUED',
               error_message = $2,
               updated_at = CURRENT_TIMESTAMP
           WHERE id = $1`,
          [jobId, errorMessage]
        );
        const headers = {
          ...(message.properties.headers || {}),
          "x-retry-count": attemptNumber
        };
        await publishConfirmed(
          channel,
          RETRY_QUEUE,
          message.content,
          {
            persistent: true,
            contentType: "application/json",
            headers
          }
        );
        console.log(
          `[Worker] Retry scheduled for ${jobId} ` +
          `(attempt ${attemptNumber + 1} after 5 seconds)`
        );
      } else {
        const deadLetter = Buffer.from(JSON.stringify({
          jobId,
          attempts: attemptNumber,
          reason: errorMessage,
          failedAt: new Date().toISOString()
        }));
        await publishConfirmed(
          channel,
          DLQ,
          deadLetter,
          {
            persistent: true,
            contentType: "application/json",
            headers: {
              "x-job-id": jobId,
              "x-attempts": attemptNumber
            }
          }
        );
        await pool.query(
          `UPDATE jobs
           SET status = 'FAILED',
               error_message = $2,
               updated_at = CURRENT_TIMESTAMP
           WHERE id = $1`,
          [jobId, errorMessage]
        );
        await publishJobEvent(channel, {
          event: "job.failed",
          jobId,
          status: "FAILED",
          attempts: attemptNumber,
          error: errorMessage,
          occurredAt: new Date().toISOString()
        });        
        console.error(
          `[Worker] Max attempts reached. Job sent to DLQ: ${jobId}`
        );
      }
      channel.ack(message);
    } catch (handlingError) {
      console.error(
        "[Worker] Could not safely schedule retry/DLQ:",
        handlingError.message
      );
      channel.nack(message, false, true);
    }
  }
}
async function startWorker() {
  if (!process.env.RABBITMQ_URL) {
    throw new Error("RABBITMQ_URL is not configured");
  }
  const connection = await amqp.connect(
    process.env.RABBITMQ_URL
  );
 const channel = await connection.createConfirmChannel();
  await channel.assertQueue(JOB_QUEUE, {
    durable: true
  });
  await channel.assertQueue(RETRY_QUEUE, {
    durable: true,
    arguments: {
      "x-message-ttl": RETRY_DELAY_MS,
      "x-dead-letter-exchange": "",
      "x-dead-letter-routing-key": JOB_QUEUE
    }
  });
  await channel.assertQueue(DLQ, {
    durable: true
  });
  await channel.assertQueue(JOB_EVENTS_QUEUE, {
    durable: true
  });
  channel.prefetch(1);
  console.log(`[Worker] Listening on ${JOB_QUEUE}`);
  console.log(`[Worker] Retry queue: ${RETRY_QUEUE}`);
  console.log(`[Worker] Dead letter queue: ${DLQ}`);
  await channel.consume(
    JOB_QUEUE,
    (message) => {
      processJob(channel, message).catch((error) => {
        console.error("[Worker] Unexpected error:", error.message);
      });
    },
    { noAck: false }
  );
  async function shutdown() {
    console.log("\n[Worker] Shutting down...");
    try {
      await channel.close();
      await connection.close();
      await pool.end();
    } catch (error) {
      console.error("[Worker] Shutdown error:", error.message);
    }
    process.exit(0);
  }
  process.once("SIGINT", shutdown);
  process.once("SIGTERM", shutdown);
}
startWorker().catch((error) => {
  console.error("[Worker] Startup failed:", error.message);
  process.exit(1);
});
