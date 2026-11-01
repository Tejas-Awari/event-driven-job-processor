const { performance } = require("node:perf_hooks");

const API_URL = process.env.API_URL || "http://localhost:3000";
const JOB_COUNT = Number(process.env.JOBS || 5);

if (
  !Number.isInteger(JOB_COUNT) ||
  JOB_COUNT < 1 ||
  JOB_COUNT > 50
) {
  console.error("JOBS must be an integer from 1 to 50.");
  process.exit(1);
}

async function submitJob(index) {
  const start = performance.now();

  try {
    const response = await fetch(`${API_URL}/jobs`, {
      method: "POST",
      headers: {
        "Content-Type": "application/json"
      },
      body: JSON.stringify({
        type: "load-test",
        message: `Load test job ${index}`
      })
    });

    const body = await response.json();

    return {
      index,
      status: response.status,
      latencyMs: Number((performance.now() - start).toFixed(2)),
      jobId: body.jobId || null,
      error: body.error || null
    };
  } catch (error) {
    return {
      index,
      status: 0,
      latencyMs: Number((performance.now() - start).toFixed(2)),
      error: error.message
    };
  }
}

function percentile(values, fraction) {
  const sorted = [...values].sort((a, b) => a - b);
  const index = Math.ceil(fraction * sorted.length) - 1;

  return sorted[Math.max(0, index)] ?? 0;
}

async function main() {
  console.log(`Submitting ${JOB_COUNT} jobs concurrently...`);

  const results = await Promise.all(
    Array.from({ length: JOB_COUNT }, (_, index) =>
      submitJob(index + 1)
    )
  );

  for (const result of results) {
    console.log(
      `Job ${result.index}: HTTP ${result.status}, ` +
      `${result.latencyMs} ms` +
      (result.jobId ? `, ID ${result.jobId}` : "") +
      (result.error ? `, Error: ${result.error}` : "")
    );
  }

  const successful = results.filter(
    (result) => result.status === 201
  );

  const latencies = successful.map((result) => result.latencyMs);

  console.log("\n--- Load test summary ---");
  console.log(`Requests submitted: ${JOB_COUNT}`);
  console.log(`Accepted (HTTP 201): ${successful.length}`);
  console.log(`Failed requests: ${JOB_COUNT - successful.length}`);

  if (latencies.length > 0) {
    const average =
      latencies.reduce((sum, value) => sum + value, 0) /
      latencies.length;

    console.log(`Average response: ${average.toFixed(2)} ms`);
    console.log(`p95 response: ${percentile(latencies, 0.95)} ms`);
  }

  if (successful.length !== JOB_COUNT) {
    process.exitCode = 1;
  }

  console.log(
    "\nNote: HTTP acceptance measures enqueue performance, " +
    "not job completion or maximum production capacity."
  );
}

main().catch((error) => {
  console.error("Load test failed:", error.message);
  process.exitCode = 1;
});
