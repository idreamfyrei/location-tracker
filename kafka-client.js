import { Kafka } from "kafkajs";

export const kafkaClient = new Kafka({
  clientId: "dreamfyrecodes",
  brokers: ["localhost:9092"],
});
