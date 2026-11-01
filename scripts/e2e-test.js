require("dotenv").config();

const assert = require("node:assert/strict");
const WebSocket = require("ws");

const API_URL = process.env.API_URL || "http://localhost:3000";
const WS_URL = process.env.WS_URL || "ws://localhost:3000";
const TIMEOUT_MS = Number(process.env.E2E_TIMEOUT_MS || 180000);

function waitForTerminalEvent(jobId) {
  return new Promise((resolve, reject) => {
    const socket = new WebSocket(WS_URL);
    let settled = false;

    const timer = setTimeout(() => {
      finish(new Error("Timed out waiting for a terminal job event"));
    }, TIMEOUT_MS);

    function finish(error, event) {
      if (settled) return;

      settled = true;
      clearTimeout(timer);

      try {
        if (socket.readyState === WebSocket.OPEN) {
          socket.close();
        } else {
          socket.terminate();
        }
      } catch {
        // Ignore cleanup errors.
      }

      if (error) reject(error);
      else resolve(event);
    }

    socket.on("open", () => {
      socket.send(JSON.stringify({
        action: "subscribe",
        jobId
      }));
    });

    socket.on("message", (raw) => {
      let event;

      try {
        event = JSON.parse(raw.toString());
      } catch {
        return;
      }

      const terminal =
        event.event === "job.completed" ||
        event.event === "job.failed" ||
        (
          event.event === "job.status" &&
          ["COMPLETED", "FAILED"].includes(event.status)
        );

      if (event.jobId === jobId && terminal) {
        finish(null, event);
      }
    });

    socket.on("error", (error) => finish(error));
  });
}

async function main() {
  console.log("1. Checking API health...");

  let response = await fetch(`${API_URL}/health`);
  assert.equal(response.status, 200);

  const health = await response.json();
  assert.equal(health.status, "ok");

  console.log("   PASS: API health");

  console.log("2. Testing invalid job ID...");

  response = await fetch(`${API_URL}/jobs/not-a-uuid`);
  assert.equal(response.status, 400);

  console.log("   PASS: Invalid ID returns 400");

  console.log("3. Testing unknown job ID...");

  response = await fetch(
    `${API_URL}/jobs/00000000-0000-4000-8000-000000000000`
  );

  assert.equal(response.status, 404);

  console.log("   PASS: Unknown job returns 404");

  console.log("4. Creating a job...");

  response = await fetch(`${API_URL}/jobs`, {
    method: "POST",
    headers: {
      "Content-Type": "application/json"
    },
    body: JSON.stringify({
      type: "e2e-test",
      message: "Milestone 9 automated test"
    })
  });

  assert.equal(
    response.status,
    201,
    `Expected 201 but received ${response.status}`
  );

  const created = await response.json();
  assert.ok(created.jobId);

  console.log(`   PASS: Created job ${created.jobId}`);

  console.log("5. Waiting for WebSocket notification...");

  const event = await waitForTerminalEvent(created.jobId);

  assert.equal(
    event.status,
    "COMPLETED",
    `Job did not complete: ${JSON.stringify(event)}`
  );

  console.log("   PASS: Received completion notification");

  console.log("6. Verifying persisted job status...");

  response = await fetch(`${API_URL}/jobs/${created.jobId}`);
  assert.equal(response.status, 200);

  const job = await response.json();

  assert.equal(job.status, "COMPLETED");
  assert.equal(job.attempts, 1);
  assert.equal(job.result, "Processing completed successfully");

  console.log("   PASS: PostgreSQL contains completed job");

  console.log("\nAll end-to-end tests passed.");
}

main().catch((error) => {
  console.error("\nTEST FAILED:", error.message);
  process.exitCode = 1;
});
