import http from "node:http";
import { fileURLToPath } from "node:url";
import path from "node:path";

import express from "express";
import { Server } from "socket.io";
import "dotenv/config";
import { kafkaClient } from "./kafka-client.js";

async function main() {
  const __filename = fileURLToPath(import.meta.url);
  const __dirname = path.dirname(__filename);
  const publicDir = path.join(__dirname, "public");
  const PORT = process.env.PORT || 3000;
  const app = express();
  const server = http.createServer(app);

  const io = new Server();

  const kafkaProducer = kafkaClient.producer();
  await kafkaProducer.connect();

  const kafkaConsumer = kafkaClient.consumer({
    groupId: `socket-server-${PORT}`,
  });
  await kafkaConsumer.connect();

  await kafkaConsumer.subscribe({
    topics: ["location-updates"],
    fromBeginning: true,
  });

  kafkaConsumer.run({
    eachMessage: async ({ topic, partition, message, heartbeat }) => {
      const data = JSON.parse(message.value.toString());
      console.log(`kafka consumer data received`, { data });
      io.emit("server:location:update", { id: data.id, latitude: data.latitude, longitude: data.longitude });
      await heartbeat();
    },
  });

  io.attach(server);

  io.on("connection", (socket) => {
    console.log(`[Socket:$(socket.id)]: connected`);

    socket.on("user:location:update", (locationData) => {
      const { latitude, longitude } = locationData;
      console.log(
        `[Socket:$(socket.id)]:user:location:update: location updated to`,
        latitude,
        longitude,
      );

      kafkaProducer.send({
        topic: "location-updates",
        messages: [
          {
            key: socket.id,
            value: JSON.stringify({
              id: socket.id,
              latitude,
              longitude,
            }),
          },
        ],
      });
    });
  });

  app.use(express.static(publicDir));
  app.get("/health", (req, res) => {
    return res.json({ status: "ok" });
  });

  server.listen(PORT, () => {
    console.log(`Server is running on port ${PORT}`);
  });
}

main();
