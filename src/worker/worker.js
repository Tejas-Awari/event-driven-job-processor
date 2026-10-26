require("dotenv").config();

const amqp = require("amqplib");
const pool = require("../db/pool");

const QUEUE_NAME = "job_queue";

function sleep(milliseconds) {
  return new Promise((resolve) => {
    setTimeout(resolve, milliseconds);
  });
}

async function processJob(channel, message) {
  if (!message) {
    return;
  }

  let jobId;

  // Read the message received from RabbitMQ.
  try {
    const payload = JSON.parse(message.content.toString());
    jobId = payload.jobId;

    if (typeof jobId !== "string" || jobId.length === 0) {
      throw new Error("Message does not contain a valid jobId");
    }
  } catch (error) {
    console.error("Invalid message:", error.message);

    // Discard malformed messages for this milestone.
    // A DLQ will be implemented in Milestone 6.
    channel.ack(message);
    return;
  }

  try {
    // Step 1: Change job status to PROCESSING.
    const started = await pool.query(
      `UPDATE jobs
       SET status = 'PROCESSING',
           attempts = attempts + 1,
           error_message = NULL,
           updated_at = CURRENT_TIMESTAMP
       WHERE id = $1
       RETURNING id`,
      [jobId]
    );

    if (started.rowCount === 0) {
      console.error(`Job not found in PostgreSQL: ${jobId}`);

      channel.ack(message);
      return;
    }

    console.log(`[Worker] Processing job: ${jobId}`);

    // Step 2: Simulate expensive processing.
    await sleep(10000);

    // Step 3: Store the result and mark the job complete.
    await pool.query(
      `UPDATE jobs
       SET status = 'COMPLETED',
           result = $2,
           error_message = NULL,
           updated_at = CURRENT_TIMESTAMP
       WHERE id = $1`,
      [jobId, "Processing completed successfully"]
    );

    console.log(`[Worker] Completed job: ${jobId}`);

    // Acknowledge the message only after processing is saved.
    channel.ack(message);
  } catch (error) {
    console.error(`[Worker] Failed job ${jobId}:`, error.message);

    try {
      // For now, record the failure without automatic retries.
      await pool.query(
        `UPDATE jobs
         SET status = 'FAILED',
             error_message = $2,
             updated_at = CURRENT_TIMESTAMP
         WHERE id = $1`,
        [jobId, error.message]
      );

      channel.ack(message);
      console.log(`[Worker] Marked job FAILED: ${jobId}`);
    } catch (databaseError) {
      console.error(
        "[Worker] Could not save the failure status:",
        databaseError.message
      );

      // Keep the message available if we cannot update PostgreSQL.
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

  const channel = await connection.createChannel();

  await channel.assertQueue(QUEUE_NAME, {
    durable: true
  });

  // Process one unacknowledged job at a time.
  channel.prefetch(1);

  console.log(`[Worker] Listening on queue: ${QUEUE_NAME}`);

  await channel.consume(
    QUEUE_NAME,
    (message) => {
      processJob(channel, message).catch((error) => {
        console.error("[Worker] Unexpected processing error:", error);
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
