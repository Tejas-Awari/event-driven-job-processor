require("dotenv").config();
const express = require("express");
const amqp = require("amqplib");
const http = require("node:http");
const { randomUUID } = require("node:crypto");
const { WebSocketServer, WebSocket } = require("ws");
const pool = require("./db/pool");
const { publishJob } = require("./queue/rabbitmq");
const app = express();
const PORT = 3000;
const JOB_EVENTS_QUEUE = "job_events";
const UUID_PATTERN =
  /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;
app.use(express.json({ limit: "100kb" }));
app.get("/health", (req, res) => {
  res.json({ status: "ok" });
});
app.post("/jobs", async (req, res) => {
  const payload = req.body;
  if (
    !payload ||
    typeof payload !== "object" ||
    Array.isArray(payload) ||
    Object.keys(payload).length === 0
  ) {
    return res.status(400).json({
      error: "Request body must be a non-empty JSON object"
    });
  }
  const jobId = randomUUID();
  let job;
  try {
    const result = await pool.query(
      `INSERT INTO jobs (id, status, payload)
       VALUES ($1, $2, $3::jsonb)
       RETURNING id, status, payload, attempts, created_at`,
      [jobId, "QUEUED", JSON.stringify(payload)]
    );
    job = result.rows[0];
  } catch (error) {
    console.error("Failed to save job:", error.message);
    return res.status(500).json({
      error: "Failed to create job"
    });
  }
  try {
    await publishJob(job.id);
  } catch (error) {
    console.error("Failed to publish job:", error.message);
    try {
      await pool.query(
        `UPDATE jobs
         SET status = 'FAILED',
             error_message = $1,
             updated_at = CURRENT_TIMESTAMP
         WHERE id = $2`,
        [`Failed to publish to RabbitMQ: ${error.message}`, job.id]
      );
    } catch (dbError) {
      console.error("Failed to update job:", dbError.message);
    }
    return res.status(503).json({
      jobId: job.id,
      status: "FAILED",
      error: "Unable to enqueue job right now"
    });
  }
  return res.status(201).json({
    jobId: job.id,
    status: job.status,
    payload: job.payload,
    attempts: job.attempts,
    createdAt: job.created_at
  });
});
// Poll job status
app.get("/jobs/:id", async (req, res) => {
  const jobId = req.params.id;
  if (!UUID_PATTERN.test(jobId)) {
    return res.status(400).json({
      error: "Invalid job ID format"
    });
  }
  try {
    const result = await pool.query(
      `SELECT id, status, payload, result, attempts,
              error_message, created_at, updated_at
       FROM jobs
       WHERE id = $1`,
      [jobId]
    );
    if (result.rowCount === 0) {
      return res.status(404).json({ error: "Job not found" });
    }
    const job = result.rows[0];
    return res.json({
      jobId: job.id,
      status: job.status,
      payload: job.payload,
      result: job.result,
      attempts: job.attempts,
      error: job.error_message,
      createdAt: job.created_at,
      updatedAt: job.updated_at
    });
  } catch (error) {
    console.error("Failed to fetch job:", error.message);
    return res.status(500).json({
      error: "Failed to fetch job status"
    });
  }
});
const httpServer = http.createServer(app);
const wss = new WebSocketServer({ server: httpServer });
wss.on("connection", (socket) => {
  socket.subscribedJobIds = new Set();
  socket.send(JSON.stringify({
    event: "connected",
    message: "Send a subscribe message with a jobId."
  }));
  socket.on("message", (raw) => {
    let request;
    try {
      request = JSON.parse(raw.toString());
    } catch {
      socket.send(JSON.stringify({
        event: "error",
        message: "Invalid JSON"
      }));
      return;
    }
    if (
      request.action !== "subscribe" ||
      typeof request.jobId !== "string" ||
      !UUID_PATTERN.test(request.jobId)
    ) {
      socket.send(JSON.stringify({
        event: "error",
        message: 'Send {"action":"subscribe","jobId":"YOUR_JOB_ID"}'
      }));
      return;
    }
    socket.subscribedJobIds.add(request.jobId);
    socket.send(JSON.stringify({
      event: "subscription.confirmed",
      jobId: request.jobId
    }));
    pool.query(
      `SELECT id, status, result, error_message
       FROM jobs
       WHERE id = $1`,
      [request.jobId]
    ).then((result) => {
      if (result.rowCount === 0) {
        return;
      }
      const job = result.rows[0];
      if (
        socket.readyState === WebSocket.OPEN &&
        socket.subscribedJobIds.has(job.id) &&
        ["COMPLETED", "FAILED"].includes(job.status)
      ) {
        socket.send(JSON.stringify({
          event: "job.status",
          jobId: job.id,
          status: job.status,
          result: job.result,
          error: job.error_message
        }));
      }
    }).catch((error) => {
      console.error("Failed to check subscribed job:", error.message);
    });
  });
  socket.on("error", (error) => {
    console.error("[WebSocket] Client error:", error.message);
  });
});
function broadcastJobEvent(event) {
  const data = JSON.stringify(event);
  for (const client of wss.clients) {
    if (
      client.readyState === WebSocket.OPEN &&
      client.subscribedJobIds?.has(event.jobId)
    ) {
      client.send(data);
    }
  }
}
async function startEventConsumer() {
  const connection = await amqp.connect(process.env.RABBITMQ_URL);
  const channel = await connection.createChannel();
  await channel.assertQueue(JOB_EVENTS_QUEUE, {
    durable: true
  });
  await channel.consume(
    JOB_EVENTS_QUEUE,
    (message) => {
      if (!message) {
        return;
      }
      try {
        const event = JSON.parse(message.content.toString());
        if (
          typeof event.jobId !== "string" ||
          typeof event.event !== "string"
        ) {
          throw new Error("Invalid job event");
        }
        broadcastJobEvent(event);
      } catch (error) {
        console.error("[API] Invalid job event:", error.message);
      }
      channel.ack(message);
    },
    { noAck: false }
  );
  connection.on("error", (error) => {
    console.error("[API] RabbitMQ event connection error:", error.message);
  });
  channel.on("error", (error) => {
    console.error("[API] RabbitMQ event channel error:", error.message);
  });
  console.log(`[API] Listening for events on ${JOB_EVENTS_QUEUE}`);
}
async function startServer() {
  try {
    await pool.query("SELECT 1");
    console.log("Connected to PostgreSQL");
    await startEventConsumer();
    httpServer.listen(PORT, () => {
      console.log(`API server running on http://localhost:${PORT}`);
      console.log(`WebSocket server running on ws://localhost:${PORT}`);
    });
  } catch (error) {
    console.error("Unable to start API:", error.message);
    process.exit(1);
  }
}
startServer();
