require("dotenv").config();

const express = require("express");
const { randomUUID } = require("node:crypto");
const pool = require("./db/pool");

const app = express();
const PORT = 3000;

app.use(express.json({ limit: "100kb" }));

// Basic API health check
app.get("/health", (req, res) => {
  res.json({
    status: "ok"
  });
});

// Create a job and save it to PostgreSQL
app.post("/jobs", async (req, res) => {
  const payload = req.body;

  // Validate the request body
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

  try {
    const query = `
      INSERT INTO jobs (id, status, payload)
      VALUES ($1, $2, $3::jsonb)
      RETURNING id, status, payload, attempts, created_at
    `;

    const values = [
      jobId,
      "QUEUED",
      JSON.stringify(payload)
    ];

    const result = await pool.query(query, values);
    const job = result.rows[0];

    return res.status(201).json({
      jobId: job.id,
      status: job.status,
      payload: job.payload,
      attempts: job.attempts,
      createdAt: job.created_at
    });
  } catch (error) {
    console.error("Failed to create job:", error.message);

    return res.status(500).json({
      error: "Failed to create job"
    });
  }
});

// Verify PostgreSQL before starting the API
async function startServer() {
  try {
    await pool.query("SELECT 1");

    console.log("Connected to PostgreSQL");

    app.listen(PORT, () => {
      console.log(`API server running on http://localhost:${PORT}`);
    });
  } catch (error) {
    console.error("Unable to connect to PostgreSQL:", error.message);
    process.exit(1);
  }
}

startServer();
