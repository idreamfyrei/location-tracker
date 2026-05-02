import http from "node:http";
import { fileURLToPath } from "node:url";
import path from "node:path";

import express from "express";
import { Server } from "socket.io";
import "dotenv/config";

async function main() {
  const __filename = fileURLToPath(import.meta.url);
  const __dirname = path.dirname(__filename);
  const publicDir = path.join(__dirname, "public");
  const PORT = process.env.PORT || 3000;
  const app = express();
  const server = http.createServer(app);

  const io = new Server();
  io.attach(server);

  io.on("connection", (socket) => {
    console.log(`[Socket:$(socket.id)]: connected`);

    socket.on("user:location:update", (locationData) => {
      const { latitude, longitude } = locationData;
        console.log(
            `[Socket:$(socket.id)]:user:location:update: location updated to`, latitude, longitude
      );
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
