const amqp = require("amqplib");

const QUEUE_NAME = "job_queue";

let channelPromise = null;

async function createChannel() {
  if (!process.env.RABBITMQ_URL) {
    throw new Error("RABBITMQ_URL is not configured");
  }

  const connection = await amqp.connect(
    process.env.RABBITMQ_URL
  );

  connection.on("error", (error) => {
    console.error("RabbitMQ connection error:", error.message);
  });

  connection.on("close", () => {
    channelPromise = null;
    console.error("RabbitMQ connection closed");
  });

  const channel = await connection.createConfirmChannel();

  channel.on("error", (error) => {
    console.error("RabbitMQ channel error:", error.message);
  });

  await channel.assertQueue(QUEUE_NAME, {
    durable: true
  });

  console.log(`RabbitMQ queue ready: ${QUEUE_NAME}`);

  return channel;
}

async function getChannel() {
  if (!channelPromise) {
    channelPromise = createChannel().catch((error) => {
      channelPromise = null;
      throw error;
    });
  }

  return channelPromise;
}

async function publishJob(jobId) {
  const channel = await getChannel();

  const message = {
    jobId: jobId
  };

  channel.sendToQueue(
    QUEUE_NAME,
    Buffer.from(JSON.stringify(message)),
    {
      persistent: true,
      contentType: "application/json",
      headers: {
       "x-retry-count": 0
     } 
    }
  );
  await channel.waitForConfirms();
}

module.exports = {
  publishJob,
  QUEUE_NAME
};
