import "dotenv/config";
import { Kafka } from "kafkajs";

const brokers = (process.env.KAFKA_BROKERS || "localhost:9092")
  .split(",")
  .map((b) => b.trim());

export const kafkaClient = new Kafka({
  clientId: "dreamfyrecodes",
  brokers,
});
