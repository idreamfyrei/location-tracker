# Location Tracker

- [IdP](https://auth.saumyagrawal.in/)


Real-time location sharing app built with Node.js, Express, Socket.IO, and Kafka.
Users authenticate through an OIDC identity provider, grant browser geolocation access, and publish live coordinates that stream to all connected clients on a shared Leaflet map.

## Project Overview

Each authenticated user's location is sent through Socket.IO to the Express server, which publishes it to a Kafka topic. A consumer on the same server reads from that topic and broadcasts the update to all connected clients. A separate `data-processor` consumer in its own consumer group reads the same events to simulate database persistence — completely independent of the real-time path.

Anonymous users can see the map and other users' live markers. Only authenticated users can share their own location.

## Tech Stack

| Layer | Tech |
|---|---|
| Backend | Node.js (ESM), Express 5 |
| Realtime | Socket.IO |
| Event streaming | Kafka via KafkaJS |
| Frontend | Vanilla HTML/CSS/JS + Leaflet |
| Auth | OIDC Authorization Code + PKCE |
| Infra (local) | Docker Compose (Kafka broker) |

## File Structure

```
location-tracker/
├── index.js              Main server — auth, sessions, socket handler, Kafka producer + consumer
├── kafka-client.js       Shared Kafka client (reads broker from env)
├── kafka-admin.js        One-time topic creation script
├── data-processor.js     Separate Kafka consumer (simulates DB writes)
├── public/
│   └── index.html        Map UI, auth banner, geolocation + socket client
├── docker-compose.yml    Local Kafka broker
├── .env.example          All environment variables with defaults
└── package.json
```

## Setup Steps

### Prerequisites

- Node.js 18+
- pnpm
- Docker Desktop

### 1. Install dependencies

```bash
pnpm install
```

### 2. Configure environment

```bash
cp .env.example .env
```

Edit `.env` with your values (see Environment Variables below).

### 3. Start Kafka

```bash
docker compose up -d
```

### 4. Create the Kafka topic

```bash
node kafka-admin.js
```

Creates the `location-updates` topic with 2 partitions. Run once.

### 5. Start the app

```bash
pnpm start
```

Open `http://localhost:3000`.

### 6. (Optional) Run the database processor

In a separate terminal:

```bash
node data-processor.js
```

This is the second Kafka consumer group. It prints `INSERT INTO DB LOCATION` for every location event — simulating what a real DB write consumer would do.

## Environment Variables

| Variable | Default | Description |
|---|---|---|
| `PORT` | `3000` | HTTP server port |
| `PUBLIC_BASE_URL` | `http://localhost:3000` | Public base URL (used to derive the OIDC redirect URI) |
| `SESSION_SECRET` | — | Secret for HMAC-signing session cookies. Set a long random string. |
| `OIDC_ISSUER_URL` | `` | Base URL of your OIDC identity provider |
| `OIDC_CLIENT_ID` | `location-tracker` | OAuth client ID registered at the IdP |
| `OIDC_REDIRECT_URI` | derived from `PUBLIC_BASE_URL` | Pin the callback URL if the derived value is wrong |
| `KAFKA_BROKERS` | `localhost:9092` | Comma-separated Kafka broker addresses |
| `LOCATION_INTERVAL_MS` | `10000` | How often the browser sends a location update (ms) |

## OIDC Auth Setup

Register a client at your IdP with:

- **Client ID:** `location-tracker` (or whatever you set in `OIDC_CLIENT_ID`)
- **Redirect URI:** `https://<your-domain>/auth/callback`
- **Grant type:** Authorization Code
- **Scopes:** `openid profile email`

### Auth flow

```
Browser                   Server                    IdP
  |                          |                        |
  |-- GET /api/me ---------->|                        |
  |<-- { authenticated:false }                        |
  |                          |                        |
  |-- GET /auth/login ------->|                        |
  |                          | generate PKCE verifier  |
  |                          | store state in memory   |
  |<-- 302 /authorize?... ---|                        |
  |                          |                        |
  |-- GET /authorize --------------------------------->|
  |<-- 302 /auth/callback?code&state -----------------|
  |                          |                        |
  |-- GET /auth/callback ---->|                        |
  |                          |-- POST /oauth/token --->|
  |                          |<-- access_token --------|
  |                          |-- GET /oauth/userinfo ->|
  |                          |<-- { sub, email, name } |
  |                          | create signed cookie    |
  |<-- 302 / + Set-Cookie ---|                        |
```

PKCE ensures the authorization code is useless if intercepted — the code verifier never leaves the server. Sessions are stored in memory with an HMAC-signed HttpOnly cookie.

## Socket Event Flow

```
Browser                    Server
  |                           |
  |-- connect (cookie) ------->|  server reads session cookie → attaches user
  |                           |
  |-- user:location:update --->|  { latitude, longitude }
  |                           |  validates auth → publishes to Kafka
  |                           |
  |<-- server:location:update -|  broadcast to ALL connected clients
  |<-- server:auth:required ---|  if socket has no session
  |<-- server:error -----------|  if Kafka is not ready
  |<-- server:user:disconnected|  when any user disconnects
```

Socket events reference:

| Event | Direction | Payload | Description |
|---|---|---|---|
| `user:location:update` | client → server | `{ latitude, longitude }` | Send current position |
| `server:location:update` | server → client | `{ id, latitude, longitude, user }` | Broadcast position update |
| `server:user:disconnected` | server → client | `{ id }` | Remove stale marker |
| `server:auth:required` | server → client | — | Unauthenticated toggle attempt |
| `server:error` | server → client | `{ message }` | Kafka unavailable |

## Kafka Event Flow

```
Browser
  └── socket emit: user:location:update
        │
        ▼
  Express Server
  └── kafkaProducer.send → topic: location-updates
        │
        ▼
  Kafka broker (location-updates, 2 partitions)
        │
        ├── Consumer Group: socket-server-{PORT}
        │     └── receives message → io.emit server:location:update
        │           └── all connected browsers update marker
        │
        └── Consumer Group: database-processor
              └── receives same message → logs INSERT INTO DB
                    └── (independent of real-time path)
```

### Why two consumer groups?

Kafka delivers every message to **each consumer group independently**. This means:

- The `socket-server` group handles real-time fan-out to clients — fast path, no blocking
- The `database-processor` group handles persistence — can batch, throttle, or retry without slowing down the socket path

If the DB consumer is slow or crashes, it has no effect on real-time updates. This is why location tracking systems (ride-hailing, delivery apps) use event streams instead of writing to the database on every socket event.

### Why not write to the DB directly from the socket handler?

At scale, thousands of users sending location every 10 seconds = thousands of DB writes per second. Direct writes would either overload the database or require complex connection pooling. Kafka absorbs the burst, and the DB consumer writes at the pace the database can handle.

## Routes

| Route | Description |
|---|---|
| `GET /` | Serves the map UI |
| `GET /health` | Health check |
| `GET /api/me` | Current session user + OIDC config + location interval |
| `GET /auth/login` | Starts OIDC login flow |
| `GET /auth/callback` | Handles IdP redirect, creates session |
| `POST /auth/logout` | Destroys session, clears cookie |

`GET /health` returns `{ "status": "ok" }`. Optional routes: `GET /auth/register` starts the IdP signup flow (same pattern as login).

## Assumptions and Limitations

- **Sessions are in-memory** — restarting the server logs everyone out. A production version would use Redis.
- **No JWT signature verification** — the IdP's access token is trusted to fetch userinfo, but the id_token signature is not verified against the IdP's JWKs endpoint.
- **Single Kafka broker** — the docker-compose setup runs one broker. Multi-broker setup requires changing `KAFKA_BROKERS`.
- **No rate limiting on socket events** — a user could flood the server with location updates. Not a concern for the demo but would be needed in production.
- **Location history is simulated** — `data-processor.js` logs events instead of writing to a real database. The consumer group architecture is correct; only the sink is stubbed.
- **Geolocation accuracy** depends on the device. For demo purposes, Chrome DevTools Sensors panel can override location coordinates.

## Demo
[Demo](https://youtu.be/QT-dy49cC88)

