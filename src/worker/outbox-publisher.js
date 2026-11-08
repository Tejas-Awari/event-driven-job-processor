
require("dotenv").config();

const amqp = require("amqplib");
const { randomUUID } = require("node:crypto");
const pool = require("../db/pool");

const QUEUE_NAME = "job_queue";

const PUBLISHER_ID = `outbox-publisher-${randomUUID()}`;

const BATCH_SIZE = 1;
const POLL_INTERVAL_MS = 1000;
const LOCK_DURATION_SECONDS = 120;
const LEASE_RENEW_INTERVAL_MS = 30000;
const MAX_BACKOFF_MS = 60000;

let connection = null;
let channel = null;
let isShuttingDown = false;
let shutdownResolve;

const shutdownSignal = new Promise((resolve) => {
  shutdownResolve = resolve;
});

const sleep = (ms) =>
  new Promise((resolve) => setTimeout(resolve, ms));

// Connect to RabbitMQ and create a confirmation channel
async function connectRabbitMQ() {
  connection = await amqp.connect(process.env.RABBITMQ_URL);

  connection.on("error", (error) => {
    console.error("[Outbox] RabbitMQ connection error:", error.message);
  });

  connection.on("close", () => {
    channel = null;
    console.error("[Outbox] RabbitMQ connection closed");
  });

  channel = await connection.createConfirmChannel();

  channel.on("error", (error) => {
    console.error("[Outbox] RabbitMQ channel error:", error.message);
  });

  await channel.assertQueue(QUEUE_NAME, {
    durable: true
  });

  console.log(`[Outbox] Connected. Queue ready: ${QUEUE_NAME}`);
}

// Claim pending events using a database lease
async function claimEvents() {
  const result = await pool.query(
    `WITH candidates AS (
       SELECT id
       FROM outbox_events
       WHERE published_at IS NULL
         AND next_attempt_at <= CURRENT_TIMESTAMP
         AND (
           locked_until IS NULL
           OR locked_until <= CURRENT_TIMESTAMP
         )
       ORDER BY created_at
       LIMIT $1
       FOR UPDATE SKIP LOCKED
     )
     UPDATE outbox_events AS events
     SET locked_until =
           CURRENT_TIMESTAMP + ($2 * INTERVAL '1 second'),
         locked_by = $3,
         attempts = events.attempts + 1
     FROM candidates
     WHERE events.id = candidates.id
     RETURNING events.*`,
    [BATCH_SIZE, LOCK_DURATION_SECONDS, PUBLISHER_ID]
  );

  return result.rows;
}
// Renew ownership of an event while it is being published
async function renewEventLease(eventId) {
  const result = await pool.query(
    `UPDATE outbox_events
     SET locked_until =
       CURRENT_TIMESTAMP + ($1 * INTERVAL '1 second')
     WHERE id = $2
       AND locked_by = $3
       AND published_at IS NULL
       AND locked_until > CURRENT_TIMESTAMP
     RETURNING id`,
    [LOCK_DURATION_SECONDS, eventId, PUBLISHER_ID]
  );

  return result.rowCount === 1;
}

// Publish one event and wait for RabbitMQ confirmation
async function publishEvent(event) {
  if (!channel) {
    throw new Error("RabbitMQ channel is unavailable");
  }

  const message = Buffer.from(
    JSON.stringify(event.payload)
  );

  const accepted = channel.sendToQueue(
    QUEUE_NAME,
    message,
    {
      persistent: true,
      contentType: "application/json",
      messageId: event.id,
      headers: {
        eventType: event.event_type,
        aggregateId: event.aggregate_id,
        eventId: event.id,
        "x-retry-count": 0
      }
    }
  );

  if (!accepted) {
    console.warn("[Outbox] RabbitMQ write buffer is full; waiting for confirmation");
  }
  await channel.waitForConfirms();
}
// Mark an event as published
async function markPublished(eventId) {
  const result = await pool.query(
    `UPDATE outbox_events
     SET published_at = CURRENT_TIMESTAMP,
         locked_until = NULL,
         locked_by = NULL,
         last_error = NULL
     WHERE id = $1
       AND locked_by = $2
       AND locked_until > CURRENT_TIMESTAMP
       AND published_at IS NULL
     RETURNING id`,
    [eventId, PUBLISHER_ID]
  );

  if (result.rowCount !== 1) {
    throw new Error(`Could not mark event ${eventId} as published: lease lost`);
  }
}

// Release failed event and schedule a retry
async function scheduleRetry(event) {
  const delay = Math.min(
    1000 * 2 ** Math.min(event.attempts - 1, 10),
    MAX_BACKOFF_MS
  );

  const result = await pool.query(
    `UPDATE outbox_events
     SET next_attempt_at =
           CURRENT_TIMESTAMP + ($1 * INTERVAL '1 millisecond'),
         locked_until = NULL,
         locked_by = NULL,
         last_error = $2
     WHERE id = $3
       AND locked_by = $4
       AND locked_until > CURRENT_TIMESTAMP
       AND published_at IS NULL
     RETURNING id`,
    [
      delay,
      event.last_error || "Unknown publishing error",
      event.id,
      PUBLISHER_ID
    ]
  );

  if (result.rowCount !== 1) {
    throw new Error(`Could not schedule retry for event ${event.id}: lease lost`);
  }

  console.warn(
    `[Outbox] Event ${event.id} retry scheduled in ${delay}ms`
  );
}

// Process claimed events
async function processBatch() {
  const events = await claimEvents();

  for (const event of events) {
    if (isShuttingDown) break;

    let leaseLost = false;
    let renewalInProgress = false;
    let renewalPromise = Promise.resolve();

    const renewalTimer = setInterval(() => {
      if (renewalInProgress || leaseLost || isShuttingDown) {
        return;
      }

      renewalInProgress = true;

      renewalPromise = (async () => {
        try {
          const renewed = await renewEventLease(event.id);

          if (!renewed) {
            leaseLost = true;
            console.error(`[Outbox] Lost lease for event ${event.id}`);
          }
        } catch (error) {
          leaseLost = true;
          console.error(
            `[Outbox] Lease renewal failed for ${event.id}:`,
            error.message
          );
        } finally {
          renewalInProgress = false;
        }
      })();
    }, LEASE_RENEW_INTERVAL_MS);

    renewalTimer.unref();

    try {
      await publishEvent(event);

      clearInterval(renewalTimer);
      await renewalPromise;

      if (leaseLost) {
        throw new Error("Event lease was lost during publishing");
      }

      await markPublished(event.id);

      console.log(`[Outbox] Published event ${event.id}`);

    } catch (error) {
      clearInterval(renewalTimer);
      await renewalPromise;

      console.error(
        `[Outbox] Failed to publish ${event.id}:`,
        error.message
      );

      event.last_error = error.message;

      if (!leaseLost) {
        try {
          await scheduleRetry(event);
        } catch (retryError) {
          console.error(
            `[Outbox] Could not schedule retry for ${event.id}:`,
            retryError.message
          );
        }
      }

      if (!channel || channel.connection?.stream?.destroyed) {
        break;
      }

    } finally {
      clearInterval(renewalTimer);
      await renewalPromise;
    }
  }
}
// Main publisher loop
let publisherPromise = null;
async function startPublisher() {
  if (!process.env.RABBITMQ_URL) {
    throw new Error("RABBITMQ_URL is not configured");
  }

  console.log(`[Outbox] Starting publisher: ${PUBLISHER_ID}`);

  while (!isShuttingDown) {
    try {
      if (!channel) {
        await connectRabbitMQ();
      }

      await processBatch();

    } catch (error) {
      console.error("[Outbox] Publisher error:", error);

      channel = null;

      if (connection) {
        try {
          await connection.close();
        } catch {
          // Connection may already be closed
        }

        connection = null;
      }
    }

    if (!isShuttingDown) {
      await Promise.race([
        sleep(POLL_INTERVAL_MS),
        shutdownSignal
      ]);
    }
  }
}

// Graceful shutdown
async function shutdown() {
  if (isShuttingDown) return;

  console.log("[Outbox] Shutdown requested...");
  isShuttingDown = true;
  shutdownResolve();
  if (publisherPromise) {
    await publisherPromise;
  }

  if (channel) {
    try {
      await channel.close();
    } catch (error) {
      console.error("[Outbox] Channel close error:", error.message);
    }
  }

  if (connection) {
    try {
      await connection.close();
    } catch (error) {
      console.error("[Outbox] Connection close error:", error.message);
    }
  }

  try {
    await pool.end();
    console.log("[Outbox] PostgreSQL pool closed.");
  } catch (error) {
    console.error("[Outbox] PostgreSQL shutdown error:", error.message);
  }

  console.log("[Outbox] Shutdown complete.");
}

process.on("SIGINT", shutdown);
process.on("SIGTERM", shutdown);

if (!process.env.RABBITMQ_URL) {
  console.error("[Outbox] RABBITMQ_URL is not configured");
  process.exitCode = 1;
  shutdown();
} else {
  publisherPromise = startPublisher();
}