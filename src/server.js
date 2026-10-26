require("dotenv").config();

const express = require("express");
const { randomUUID } = require("node:crypto");
const pool = require("./db/pool");
const { publishJob } = require("./queue/rabbitmq");

const app = express();
const PORT = 3000;

app.use(express.json({ limit: "100kb" }));

app.get("/health", (req, res) => {
  res.json({
    status: "ok"
  });
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

  // First, persist the job in PostgreSQL.
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

  // Next, publish the job ID to RabbitMQ.
  try {
    await publishJob(job.id);
  } catch (error) {
    console.error("Failed to publish job:", error.message);

    // Record the enqueue failure in PostgreSQL.
    try {
      await pool.query(
        `UPDATE jobs
         SET status = $1,
             error_message = $2,
             updated_at = CURRENT_TIMESTAMP
         WHERE id = $3`,
        [
          "FAILED",
          `Failed to publish to RabbitMQ: ${error.message}`,
          job.id
        ]
      );
    } catch (dbError) {
      console.error(
        "Failed to update job after publish error:",
        dbError.message
      );
    }

    return res.status(503).json({
      jobId: job.id,
      status: "FAILED",
      error: "Unable to enqueue job right now"
    });
  }

  // Return success only after RabbitMQ confirms publication.
  return res.status(201).json({
    jobId: job.id,
    status: job.status,
    payload: job.payload,
    attempts: job.attempts,
    createdAt: job.created_at
  });
});

async function startServer() {
  try {
    await pool.query("SELECT 1");

    console.log("Connected to PostgreSQL");

    app.listen(PORT, () => {
      console.log(`API server running on http://localhost:${PORT}`);
    });
  } catch (error) {
    console.error(
      "Unable to connect to PostgreSQL:",
      error.message
    );

    process.exit(1);
  }
}

startServer();
