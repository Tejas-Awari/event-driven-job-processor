
require("dotenv").config();

const amqp = require("amqplib");
const pool = require("../db/pool");
const { randomUUID } = require("node:crypto");

const JOB_EVENTS_QUEUE = "job_events";
const JOB_QUEUE = "job_queue";
const RETRY_QUEUE = "job_retry_queue";
const DLQ = "job_dead_letter_queue";

const MAX_ATTEMPTS = 3;
const RETRY_DELAY_MS = 5000;
const LEASE_DURATION_SECONDS = 60;
const JOB_TIMEOUT_MS = 30000;
const HEARTBEAT_INTERVAL_MS = 15000;
const RECOVERY_INTERVAL_MS = 10000;
const RECOVERY_BATCH_SIZE = 25;

const WORKER_ID = `worker-${randomUUID()}`;

class LeaseLostError extends Error {
  constructor(message) {
    super(message);
    this.name = "LeaseLostError";
  }
}

function sleep(milliseconds) {
  return new Promise((resolve) => setTimeout(resolve, milliseconds));
}
function sleepWithAbort(milliseconds, signal) {
  return new Promise((resolve, reject) => {
    if (signal.aborted) {
      reject(new Error("Job processing aborted"));
      return;
    }

    const timer = setTimeout(() => {
      cleanup();
      resolve();
    }, milliseconds);

    function onAbort() {
      clearTimeout(timer);
      cleanup();
      reject(new Error("Job processing aborted"));
    }

    function cleanup() {
      signal.removeEventListener("abort", onAbort);
    }

    signal.addEventListener("abort", onAbort, { once: true });
  });
}

async function renewJobLease(jobId) {
  const result = await pool.query(
    `UPDATE jobs
     SET lease_expires_at = CURRENT_TIMESTAMP + ($3 * INTERVAL '1 second'),
         updated_at = CURRENT_TIMESTAMP
     WHERE id = $1
       AND status = 'PROCESSING'
       AND worker_id = $2
       AND lease_expires_at > CURRENT_TIMESTAMP
     RETURNING id`,
    [jobId, WORKER_ID, LEASE_DURATION_SECONDS]
  );

  return result.rowCount === 1;
}

function startHeartbeat(jobId) {
  let stopped = false;
  let leaseLost = false;
  let timer;

  const stop = () => {
    stopped = true;
    clearTimeout(timer);
  };

  const loseLease = (reason) => {
    leaseLost = true;
    console.error(`[Worker] Lease no longer safe for ${jobId}: ${reason}`);
    stop();
  };

  const heartbeat = async () => {
    if (stopped) return;

    try {
      const renewed = await renewJobLease(jobId);

      if (!renewed) {
        loseLease("lease expired or ownership changed");
        return;
      }
    } catch (error) {
      loseLease(`heartbeat database error: ${error.message}`);
      return;
    }

    if (!stopped) {
      timer = setTimeout(heartbeat, HEARTBEAT_INTERVAL_MS);
    }
  };

  timer = setTimeout(heartbeat, HEARTBEAT_INTERVAL_MS);

  return {
    stop,
    hasLostLease: () => leaseLost
  };
}
async function runWithTimeout(task, timeoutMs, externalSignal) {
  const controller = new AbortController();

  let timeoutId;
  let externalAbortHandler;

  const timeoutPromise = new Promise((_, reject) => {
    timeoutId = setTimeout(() => {
      controller.abort();

      reject(
        new Error(`Job timed out after ${timeoutMs}ms`)
      );
    }, timeoutMs);
  });

  const shutdownPromise = externalSignal
    ? new Promise((_, reject) => {
        externalAbortHandler = () => {
          controller.abort();

          reject(
            new Error("Job processing interrupted by worker shutdown")
          );
        };

        if (externalSignal.aborted) {
          externalAbortHandler();
        } else {
          externalSignal.addEventListener(
            "abort",
            externalAbortHandler,
            { once: true }
          );
        }
      })
    : new Promise(() => {});

  try {
    return await Promise.race([
      task(controller.signal),
      timeoutPromise,
      shutdownPromise
    ]);
  } finally {
    clearTimeout(timeoutId);

    if (externalSignal && externalAbortHandler) {
      externalSignal.removeEventListener(
        "abort",
        externalAbortHandler
      );
    }
  }
}

async function publishConfirmed(channel, queue, content, options = {}) {
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

async function sendInvalidMessageToDlq(channel, message, reason) {
  await publishConfirmed(
    channel,
    DLQ,
    Buffer.from(JSON.stringify({
      reason,
      originalMessage: message.content.toString(),
      failedAt: new Date().toISOString()
    })),
    {
      persistent: true,
      contentType: "application/json"
    }
  );

  channel.ack(message);
}

async function markJobFailedAndPublishDlq(
  channel,
  jobId,
  attemptNumber,
  errorMessage
) {
  const client = await pool.connect();
  let committed = false;

  try {
    await client.query("BEGIN");

    const failed = await client.query(
      `UPDATE jobs
       SET status = 'FAILED',
           error_message = $2,
           processing_started_at = NULL,
           lease_expires_at = NULL,
           worker_id = NULL,
           updated_at = CURRENT_TIMESTAMP
       WHERE id = $1
         AND status = 'PROCESSING'
         AND worker_id = $3
         AND lease_expires_at > CURRENT_TIMESTAMP
       RETURNING id`,
      [jobId, errorMessage, WORKER_ID]
    );

    if (failed.rowCount !== 1) {
      await client.query("ROLLBACK");
      return false;
    }

    const deadLetter = Buffer.from(JSON.stringify({
      jobId,
      attempts: attemptNumber,
      reason: errorMessage,
      failedAt: new Date().toISOString()
    }));

    await publishConfirmed(channel, DLQ, deadLetter, {
      persistent: true,
      contentType: "application/json",
      headers: {
        "x-job-id": jobId,
        "x-attempts": attemptNumber
      }
    });

    await client.query("COMMIT");
    committed = true;

    return true;
  } catch (error) {
    if (!committed) {
      try {
        await client.query("ROLLBACK");
      } catch (_) {
        // Database connection may already be unavailable.
      }
    }

    throw error;
  } finally {
    client.release();
  }
}

async function requeueJobForRetry(
  channel,
  message,
  jobId,
  attemptNumber,
  errorMessage
) {
  const client = await pool.connect();
  let committed = false;

  try {
    await client.query("BEGIN");

    const requeued = await client.query(
      `UPDATE jobs
       SET status = 'QUEUED',
           error_message = $2,
           processing_started_at = NULL,
           lease_expires_at = NULL,
           worker_id = NULL,
           updated_at = CURRENT_TIMESTAMP
       WHERE id = $1
         AND status = 'PROCESSING'
         AND worker_id = $3
         AND lease_expires_at > CURRENT_TIMESTAMP
       RETURNING id`,
      [jobId, errorMessage, WORKER_ID]
    );

    if (requeued.rowCount !== 1) {
      await client.query("ROLLBACK");
      return false;
    }

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

    await client.query("COMMIT");
    committed = true;

    return true;
  } catch (error) {
    if (!committed) {
      try {
        await client.query("ROLLBACK");
      } catch (_) {
        // Database connection may already be unavailable.
      }
    }

    throw error;
  } finally {
    client.release();
  }
}

async function processJob(channel, message,shutdownSignal) {
  if (!message) return;

  let jobId;
  let retryCount;
  let claimed = false;
  let heartbeat;

  try {
    const received = JSON.parse(message.content.toString());
    jobId = received.jobId;

    if (typeof jobId !== "string" || jobId.length === 0) {
      throw new Error("Message has no valid jobId");
    }

    const headerValue =
      message.properties.headers?.["x-retry-count"] ?? 0;

    retryCount = Number(headerValue);

    if (
      !Number.isInteger(retryCount) ||
      retryCount < 0 ||
      retryCount > MAX_ATTEMPTS
    ) {
      throw new Error("Invalid retry-count header");
    }
  } catch (error) {
    console.error("[Worker] Invalid message:", error.message);

    try {
      await sendInvalidMessageToDlq(channel, message, error.message);
    } catch (publishError) {
      console.error(
        "[Worker] Could not send invalid message to DLQ:",
        publishError.message
      );

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
           processing_started_at = CURRENT_TIMESTAMP,
           lease_expires_at = CURRENT_TIMESTAMP + ($4 * INTERVAL '1 second'),
           worker_id = $3,
           updated_at = CURRENT_TIMESTAMP
       WHERE id = $1
         AND status = 'QUEUED'
         AND attempts = $2
       RETURNING id, payload, attempts`,
      [jobId, retryCount, WORKER_ID, LEASE_DURATION_SECONDS]
    );

    if (started.rowCount !== 1) {
      console.log(
        `[Worker] Job not claimable; acknowledging stale delivery: ${jobId}`
      );

      channel.ack(message);
      return;
    }

    const job = started.rows[0];
    claimed = true;

    const actualAttempt = job.attempts;

    heartbeat = startHeartbeat(jobId);

    console.log(
      `[Worker] Processing ${jobId}, attempt ${actualAttempt}/${MAX_ATTEMPTS}`
    );

    if (
      job.payload?.payload?.failAlways === true ||
      job.payload?.failAlways === true
    ) {
      throw new Error("Simulated failure: failAlways is true");
    }
    await runWithTimeout(
      async (signal) => {
        const interval = 250;
        let elapsed = 0;

        const requestedProcessingTime = Number(
          job.payload?.processingTimeMs ??
          job.payload?.payload?.processingTimeMs ??
          10000
        );

        if (
          !Number.isFinite(requestedProcessingTime) ||
          requestedProcessingTime < 0
        ) {
          throw new Error("Invalid processing time");
        }

        const processingTime = requestedProcessingTime;

        while (elapsed < processingTime) {
          if (signal.aborted) {
            throw new Error("Job processing aborted");
          }

          if (heartbeat.hasLostLease()) {
            throw new LeaseLostError(
              "Worker lost its lease during processing"
            );
          }

          const duration = Math.min(
            interval,
            processingTime - elapsed
          );

          await sleepWithAbort(duration, signal);
          elapsed += duration;
        }
      },
      JOB_TIMEOUT_MS,
      shutdownSignal
    );


    if (heartbeat.hasLostLease()) {
      throw new LeaseLostError("Lease lost before completion");
    }

    const resultText = "Processing completed successfully";

    const completed = await pool.query(
      `UPDATE jobs
       SET status = 'COMPLETED',
           result = $2,
           error_message = NULL,
           processing_started_at = NULL,
           lease_expires_at = NULL,
           worker_id = NULL,
           updated_at = CURRENT_TIMESTAMP
       WHERE id = $1
         AND status = 'PROCESSING'
         AND worker_id = $3
         AND lease_expires_at > CURRENT_TIMESTAMP
       RETURNING id`,
      [jobId, resultText, WORKER_ID]
    );

    if (completed.rowCount !== 1) {
      throw new LeaseLostError(
        `Job ${jobId}: lease lost or completion rejected`
      );
    }

    heartbeat.stop();

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
        // During forced shutdown, do not consume a retry attempt.
    // Return the unacknowledged message to RabbitMQ.
    if (shutdownSignal?.aborted) {
      if (heartbeat) {
        heartbeat.stop();
      }

      console.warn(
        `[Worker] Shutdown interrupted job ${jobId || "unknown"}; requeuing message`
      );

      try {
        channel.nack(message, false, true);
      } catch (nackError) {
        console.error(
          "[Worker] Could not requeue interrupted message:",
          nackError.message
        );
      }

      return;
    }
    const errorMessage = String(error.message || error).slice(0, 2000);

    console.error(
      `[Worker] Attempt ${attemptNumber} failed for ${jobId}:`,
      errorMessage
    );

    // The job was never claimed. Keep the RabbitMQ message available.
    if (!claimed) {
      channel.nack(message, false, true);
      return;
    }

    // Ownership is uncertain. Let the recovery process handle the job
    // after its lease expires.
    if (
      error instanceof LeaseLostError ||
      heartbeat?.hasLostLease()
    ) {
      channel.ack(message);
      return;
    }

    try {
      if (attemptNumber < MAX_ATTEMPTS) {
        const requeued = await requeueJobForRetry(
          channel,
          message,
          jobId,
          attemptNumber,
          errorMessage
        );

        if (!requeued) {
          console.warn(
            `[Worker] Retry skipped because lease was no longer owned: ${jobId}`
          );

          channel.ack(message);
          return;
        }

        console.log(
          `[Worker] Retry scheduled for ${jobId} (attempt ${attemptNumber + 1})`
        );
      } else {
        const failed = await markJobFailedAndPublishDlq(
          channel,
          jobId,
          attemptNumber,
          errorMessage
        );

        if (!failed) {
          console.warn(
            `[Worker] DLQ transition skipped because lease was no longer owned: ${jobId}`
          );

          channel.ack(message);
          return;
        }

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

async function recoverExpiredJobs(channel) {
  const client = await pool.connect();
  let committed = false;
  let recoveredCount = 0;

  try {
    await client.query("BEGIN");

    const expired = await client.query(
      `SELECT id, attempts, payload
       FROM jobs
       WHERE status = 'PROCESSING'
         AND lease_expires_at <= CURRENT_TIMESTAMP
       ORDER BY lease_expires_at
       LIMIT $1
       FOR UPDATE SKIP LOCKED`,
      [RECOVERY_BATCH_SIZE]
    );

    for (const job of expired.rows) {
      if (job.attempts >= MAX_ATTEMPTS) {
        const reason = "Worker lease expired after maximum attempts";

        const failed = await client.query(
          `UPDATE jobs
           SET status = 'FAILED',
               error_message = $2,
               processing_started_at = NULL,
               lease_expires_at = NULL,
               worker_id = NULL,
               updated_at = CURRENT_TIMESTAMP
           WHERE id = $1
             AND status = 'PROCESSING'
             AND lease_expires_at <= CURRENT_TIMESTAMP
           RETURNING id`,
          [job.id, reason]
        );

        if (failed.rowCount === 1) {
          await publishConfirmed(
            channel,
            DLQ,
            Buffer.from(JSON.stringify({
              jobId: job.id,
              attempts: job.attempts,
              reason,
              failedAt: new Date().toISOString()
            })),
            {
              persistent: true,
              contentType: "application/json",
              headers: {
                "x-job-id": job.id,
                "x-attempts": job.attempts
              }
            }
          );

          recoveredCount += 1;
        }

        continue;
      }

      const requeued = await client.query(
        `UPDATE jobs
         SET status = 'QUEUED',
             processing_started_at = NULL,
             lease_expires_at = NULL,
             worker_id = NULL,
             updated_at = CURRENT_TIMESTAMP
         WHERE id = $1
           AND status = 'PROCESSING'
           AND lease_expires_at <= CURRENT_TIMESTAMP
         RETURNING id`,
        [job.id]
      );

      if (requeued.rowCount === 1) {
        await publishConfirmed(
          channel,
          JOB_QUEUE,
          Buffer.from(JSON.stringify({ jobId: job.id })),
          {
            persistent: true,
            contentType: "application/json",
            headers: {
              "x-retry-count": job.attempts
            }
          }
        );

        recoveredCount += 1;
      }
    }

    await client.query("COMMIT");
    committed = true;
  } catch (error) {
    if (!committed) {
      try {
        await client.query("ROLLBACK");
      } catch (_) {
        // Database connection may already be unavailable.
      }
    }

    throw error;
  } finally {
    client.release();
  }

  if (recoveredCount > 0) {
    console.log(
      `[Worker] Recovered ${recoveredCount} expired job(s)`
    );
  }
}
  async function startWorker() {
    if (!process.env.RABBITMQ_URL) {
      throw new Error("RABBITMQ_URL is not configured");
    }

    const SHUTDOWN_GRACE_PERIOD_MS = 35000;
    const MAX_RECONNECT_DELAY_MS = 30000;

    let connection = null;
    let channel = null;
    let consumerTag = null;

    let recoveryTimer = null;
    let reconnectTimer = null;

    let shuttingDown = false;
    let reconnectAttempts = 0;
    let recoveryInProgress = false;
    let recoveryPromise = Promise.resolve();

    // Tracks every job handler currently running.
    const activeJobs = new Set();

    // Aborted only if the graceful shutdown deadline expires.
    const shutdownController = new AbortController();

    function stopRecoveryTimer() {
      if (recoveryTimer) {
        clearInterval(recoveryTimer);
        recoveryTimer = null;
      }
    }

    function scheduleReconnect(reason) {
      if (shuttingDown || reconnectTimer) return;

      stopRecoveryTimer();

      const delay = Math.min(
        1000 * (2 ** reconnectAttempts),
        MAX_RECONNECT_DELAY_MS
      );

      reconnectAttempts += 1;

      console.error(
        `[Worker] ${reason}. Reconnecting in ${delay / 1000}s...`
      );

      reconnectTimer = setTimeout(() => {
        reconnectTimer = null;
        connectAndConsume();
      }, delay);
    }

    async function connectAndConsume() {
      if (shuttingDown) return;

      let newConnection;
      let newChannel;

      try {
        console.log("[Worker] Connecting to RabbitMQ...");

        newConnection = await amqp.connect(
          process.env.RABBITMQ_URL
        );

        if (shuttingDown) {
          await newConnection.close();
          return;
        }

        connection = newConnection;

        newConnection.on("error", (error) => {
          console.error(
            "[Worker] RabbitMQ connection error:",
            error.message
          );
        });

        newConnection.on("close", () => {
          if (connection !== newConnection || shuttingDown) return;

          connection = null;
          channel = null;
          consumerTag = null;

          scheduleReconnect("RabbitMQ connection closed");
        });

        newChannel = await newConnection.createConfirmChannel();

        if (shuttingDown) {
          await newChannel.close();
          await newConnection.close();
          return;
        }

        channel = newChannel;

        newChannel.on("error", (error) => {
          console.error(
            "[Worker] RabbitMQ channel error:",
            error.message
          );
        });

        newChannel.on("close", () => {
          if (channel !== newChannel || shuttingDown) return;

          channel = null;
          connection = null;
          consumerTag = null;

          stopRecoveryTimer();

          newConnection.close().catch(() => {});

          scheduleReconnect("RabbitMQ channel closed");
        });

        await newChannel.assertQueue(JOB_QUEUE, {
          durable: true
        });

        await newChannel.assertQueue(RETRY_QUEUE, {
          durable: true,
          arguments: {
            "x-message-ttl": RETRY_DELAY_MS,
            "x-dead-letter-exchange": "",
            "x-dead-letter-routing-key": JOB_QUEUE
          }
        });

        await newChannel.assertQueue(DLQ, {
          durable: true
        });

        await newChannel.assertQueue(JOB_EVENTS_QUEUE, {
          durable: true
        });

        newChannel.prefetch(1);

        try {
          await recoverExpiredJobs(newChannel);
        } catch (error) {
          console.error(
            "[Worker] Initial expired-job recovery failed:",
            error.message
          );
        }

        if (shuttingDown || channel !== newChannel) return;

        stopRecoveryTimer();

        recoveryTimer = setInterval(() => {
          if (
            shuttingDown ||
            channel !== newChannel ||
            recoveryInProgress
          ) {
            return;
          }

          recoveryInProgress = true;

          recoveryPromise = recoverExpiredJobs(newChannel)
            .catch((error) => {
              console.error(
                "[Worker] Expired-job recovery failed:",
                error.message
              );
            })
            .finally(() => {
              recoveryInProgress = false;
            });
        }, RECOVERY_INTERVAL_MS);

        const consumer = await newChannel.consume(
          JOB_QUEUE,
          (message) => {
            if (!message) return;

            let jobPromise;

            jobPromise = processJob(
              newChannel,
              message,
              shutdownController.signal
            )
              .catch((error) => {
                console.error(
                  "[Worker] Unexpected processing error:",
                  error.message
                );

                try {
                  newChannel.nack(message, false, true);
                } catch (_) {
                  // The channel may already be closed.
                }
              })
              .finally(() => {
                activeJobs.delete(jobPromise);
              });

            activeJobs.add(jobPromise);
          },
          {
            noAck: false
          }
        );

        consumerTag = consumer.consumerTag;

        if (shuttingDown) {
          await newChannel.cancel(consumerTag);
          consumerTag = null;
          return;
        }

        reconnectAttempts = 0;

        console.log(
          `[Worker] ${WORKER_ID} listening on ${JOB_QUEUE}`
        );
        console.log(`[Worker] Retry queue: ${RETRY_QUEUE}`);
        console.log(`[Worker] Dead letter queue: ${DLQ}`);
        console.log("[Worker] RabbitMQ connection established.");
      } catch (error) {
        console.error(
          "[Worker] RabbitMQ setup failed:",
          error.message
        );

        stopRecoveryTimer();

        if (channel === newChannel) {
          channel = null;
          consumerTag = null;
        }

        if (connection === newConnection) {
          connection = null;
        }

        try {
          if (newChannel) {
            await newChannel.close();
          }
        } catch (_) {
          // Channel may already be closed.
        }

        try {
          if (newConnection) {
            await newConnection.close();
          }
        } catch (_) {
          // Connection may already be closed.
        }

        scheduleReconnect("RabbitMQ connection/setup failed");
      }
    }

    async function waitForActiveJobs(timeoutMs) {
      if (activeJobs.size === 0) {
        return true;
      }

      let timeoutId;

      const jobsFinished = Promise.allSettled(
        [...activeJobs]
      ).then(() => true);

      const deadlineReached = new Promise((resolve) => {
        timeoutId = setTimeout(() => {
          resolve(false);
        }, timeoutMs);
      });

      const completedInTime = await Promise.race([
        jobsFinished,
        deadlineReached
      ]);

      clearTimeout(timeoutId);

      return completedInTime && activeJobs.size === 0;
    }

    async function shutdown(signalName) {
      if (shuttingDown) return;

      shuttingDown = true;

      console.log(
        `\n[Worker] Received ${signalName}. Starting graceful shutdown...`
      );

      stopRecoveryTimer();

      if (reconnectTimer) {
        clearTimeout(reconnectTimer);
        reconnectTimer = null;
      }

      // Stop RabbitMQ from delivering additional jobs.
      if (channel && consumerTag) {
        try {
          await channel.cancel(consumerTag);
          console.log("[Worker] RabbitMQ consumer cancelled.");
        } catch (error) {
          console.error(
            "[Worker] Could not cancel consumer:",
            error.message
          );
        }

        consumerTag = null;
      }

      // Give currently running jobs up to 35 seconds to finish.
      console.log(
        `[Worker] Waiting up to ${SHUTDOWN_GRACE_PERIOD_MS / 1000} seconds for ${activeJobs.size} active job(s)...`
      );

      const jobsFinished = await waitForActiveJobs(
        SHUTDOWN_GRACE_PERIOD_MS
      );

      if (jobsFinished) {
        console.log("[Worker] All active jobs finished.");
      } else {
        console.warn(
          "[Worker] Graceful shutdown deadline reached. Aborting active job processing..."
        );

        // The processJob catch block will nack interrupted messages.
        shutdownController.abort();

        // Wait for the handlers to settle before closing the channel
        // and database pool.
        await Promise.allSettled([...activeJobs]);

        console.log(
          "[Worker] Active job handlers have settled."
        );
      }

      // Do not close PostgreSQL while recovery work is still running.
      try {
        await recoveryPromise;
      } catch (error) {
        console.error(
          "[Worker] Recovery shutdown error:",
          error.message
        );
      }

      try {
        if (channel) {
          await channel.close();
          console.log("[Worker] RabbitMQ channel closed.");
        }
      } catch (error) {
        console.error(
          "[Worker] Channel shutdown error:",
          error.message
        );
      }

      try {
        if (connection) {
          await connection.close();
          console.log("[Worker] RabbitMQ connection closed.");
        }
      } catch (error) {
        console.error(
          "[Worker] Connection shutdown error:",
          error.message
        );
      }

      try {
        await pool.end();
        console.log("[Worker] PostgreSQL pool closed.");
      } catch (error) {
        console.error(
          "[Worker] PostgreSQL shutdown error:",
          error.message
        );
      }

      console.log("[Worker] Graceful shutdown complete.");
    }

    process.once("SIGINT", () => {
      shutdown("SIGINT").catch((error) => {
        console.error(
          "[Worker] Shutdown failed:",
          error.message
        );
        process.exitCode = 1;
      });
    });

    process.once("SIGTERM", () => {
      shutdown("SIGTERM").catch((error) => {
        console.error(
          "[Worker] Shutdown failed:",
          error.message
        );
        process.exitCode = 1;
      });
    });

    await connectAndConsume();
  }

startWorker().catch((error) => {
  console.error("[Worker] Startup failed:", error.message);
  process.exit(1);
});
