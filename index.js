import http from "node:http";
import crypto from "node:crypto";
import { fileURLToPath } from "node:url";
import path from "node:path";

import express from "express";
import { Server } from "socket.io";
import "dotenv/config";
import { kafkaClient } from "./kafka-client.js";

const SESSION_COOKIE = "location_tracker_sid";
const sessionStore = new Map();
const oidcStateStore = new Map();
let cachedOidcConfig = null;

function base64Url(input) {
  return Buffer.from(input).toString("base64url");
}

function createRandomString(bytes = 32) {
  return crypto.randomBytes(bytes).toString("base64url");
}

function getSessionSecret() {
  return process.env.SESSION_SECRET || "local-location-tracker-dev-secret";
}

function signValue(value) {
  return crypto
    .createHmac("sha256", getSessionSecret())
    .update(value)
    .digest("base64url");
}

function safeCompare(a, b) {
  const aBuffer = Buffer.from(a);
  const bBuffer = Buffer.from(b);
  return aBuffer.length === bBuffer.length && crypto.timingSafeEqual(aBuffer, bBuffer);
}

function parseCookies(cookieHeader = "") {
  return Object.fromEntries(
    cookieHeader
      .split(";")
      .map((cookie) => cookie.trim())
      .filter(Boolean)
      .map((cookie) => {
        const [name, ...valueParts] = cookie.split("=");
        return [decodeURIComponent(name), decodeURIComponent(valueParts.join("="))];
      }),
  );
}

function getSessionId(req) {
  const signedSessionId = parseCookies(req.headers.cookie)[SESSION_COOKIE];
  if (!signedSessionId) return null;

  const [sessionId, signature] = signedSessionId.split(".");
  if (!sessionId || !signature || !safeCompare(signValue(sessionId), signature)) {
    return null;
  }

  return sessionId;
}

function getUserFromRequest(req) {
  const sessionId = getSessionId(req);
  if (!sessionId) return null;
  const session = sessionStore.get(sessionId);
  return session?.user || null;
}

function getInitials(nameOrEmail = "") {
  const parts = nameOrEmail
    .replace(/@.*/, "")
    .split(/[\s._-]+/)
    .filter(Boolean);

  const initials = parts.slice(0, 2).map((part) => part[0]?.toUpperCase()).join("");
  return initials || "U";
}

function normalizeUser(profile, provider) {
  const name = profile.name || profile.preferred_username || profile.email || "Local user";
  const email = profile.email || "";

  return {
    id: profile.sub || profile.id || email || createRandomString(10),
    name,
    email,
    initials: getInitials(name || email),
    provider,
  };
}

function setSessionCookie(res, sessionId) {
  const signedSessionId = `${sessionId}.${signValue(sessionId)}`;
  const isSecure = (process.env.APP_URL || process.env.PUBLIC_BASE_URL || "").startsWith("https://");
  res.cookie(SESSION_COOKIE, signedSessionId, {
    httpOnly: true,
    sameSite: "lax",
    secure: isSecure,
    maxAge: 8 * 60 * 60 * 1000,
  });
}

function createSession(res, user) {
  const sessionId = createRandomString();
  sessionStore.set(sessionId, {
    user,
    createdAt: Date.now(),
  });
  setSessionCookie(res, sessionId);
}

function getPublicBaseUrl(req) {
  return (process.env.APP_URL || process.env.PUBLIC_BASE_URL || `${req.protocol}://${req.get("host")}`).replace(
    /\/+$/,
    "",
  );
}

function isSafeLocalPath(pathname) {
  return typeof pathname === "string" && pathname.startsWith("/") && !pathname.startsWith("//");
}

function getIdpUrl() {
  return (process.env.IDP_URL || process.env.OIDC_ISSUER_URL || "http://localhost:3000").replace(
    /\/+$/,
    "",
  );
}

function getClientId() {
  return process.env.CLIENT_ID || process.env.OIDC_CLIENT_ID || "location-tracker";
}

function getRedirectUri(req) {
  if (process.env.OIDC_REDIRECT_URI) return process.env.OIDC_REDIRECT_URI;
  return `${getPublicBaseUrl(req)}/auth/callback`;
}

async function getOidcConfig() {
  if (cachedOidcConfig) return cachedOidcConfig;

  const idpUrl = getIdpUrl();
  cachedOidcConfig = {
    authorization_endpoint: process.env.OIDC_AUTHORIZATION_URL || `${idpUrl}/authorize`,
    signup_endpoint: process.env.OIDC_SIGNUP_URL || `${idpUrl}/signup.html`,
    token_endpoint: process.env.OIDC_TOKEN_URL || `${idpUrl}/oauth/token`,
    userinfo_endpoint: process.env.OIDC_USERINFO_URL || `${idpUrl}/oauth/userinfo`,
  };
  return cachedOidcConfig;
}

async function exchangeCodeForTokens({ code, codeVerifier, redirectUri }) {
  const oidcConfig = await getOidcConfig();
  const body = {
    grant_type: "authorization_code",
    code,
    redirect_uri: redirectUri,
    client_id: getClientId(),
    code_verifier: codeVerifier,
  };

  if (process.env.OIDC_CLIENT_SECRET) {
    body.client_secret = process.env.OIDC_CLIENT_SECRET;
  }

  const response = await fetch(oidcConfig.token_endpoint, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify(body),
  });

  if (!response.ok) {
    throw new Error(`OIDC token exchange failed with ${response.status}`);
  }

  return response.json();
}

function decodeJwtPayload(token) {
  if (!token) return {};
  const [, payload] = token.split(".");
  if (!payload) return {};
  return JSON.parse(Buffer.from(payload, "base64url").toString("utf8"));
}

async function getOidcProfile(tokens) {
  const oidcConfig = await getOidcConfig();
  if (tokens.access_token && oidcConfig.userinfo_endpoint) {
    const response = await fetch(oidcConfig.userinfo_endpoint, {
      headers: { authorization: `Bearer ${tokens.access_token}` },
    });

    if (response.ok) {
      return response.json();
    }
  }

  return decodeJwtPayload(tokens.id_token);
}

async function buildAuthorizeUrl(req, targetEndpoint) {
  const oidcConfig = await getOidcConfig();
  const state = createRandomString(24);
  const nonce = createRandomString(16);
  const codeVerifier = createRandomString(48);
  const codeChallenge = base64Url(crypto.createHash("sha256").update(codeVerifier).digest());
  const redirectUri = getRedirectUri(req);
  const returnTo = isSafeLocalPath(req.query.returnTo) ? req.query.returnTo : "/";

  oidcStateStore.set(state, {
    codeVerifier,
    nonce,
    redirectUri,
    returnTo,
    createdAt: Date.now(),
  });

  const authUrl = new URL(targetEndpoint || oidcConfig.authorization_endpoint);
  authUrl.searchParams.set("client_id", getClientId());
  authUrl.searchParams.set("redirect_uri", redirectUri);
  authUrl.searchParams.set("response_type", "code");
  authUrl.searchParams.set("scope", process.env.OIDC_SCOPES || "openid profile email");
  authUrl.searchParams.set("state", state);
  authUrl.searchParams.set("nonce", nonce);
  authUrl.searchParams.set("code_challenge", codeChallenge);
  authUrl.searchParams.set("code_challenge_method", "S256");

  return authUrl.toString();
}

async function main() {
  const __filename = fileURLToPath(import.meta.url);
  const __dirname = path.dirname(__filename);
  const publicDir = path.join(__dirname, "public");
  const PORT = process.env.PORT || 3000;
  const app = express();
  const server = http.createServer(app);

  app.use(express.urlencoded({ extended: false }));
  app.use(express.json());

  const io = new Server();
  io.attach(server);

  const kafkaProducer = kafkaClient.producer();
  const kafkaConsumer = kafkaClient.consumer({
    groupId: `socket-server-${PORT}`,
  });

  let kafkaReady = false;

  (async () => {
    try {
      await kafkaProducer.connect();
      await kafkaConsumer.connect();
      await kafkaConsumer.subscribe({
        topics: ["location-updates"],
        fromBeginning: true,
      });
      kafkaConsumer.run({
        eachMessage: async ({ message, heartbeat }) => {
          const data = JSON.parse(message.value.toString());
          console.log(`kafka consumer data received`, { data });
          io.emit("server:location:update", {
            id: data.id,
            latitude: data.latitude,
            longitude: data.longitude,
            user: data.user,
          });
          await heartbeat();
        },
      });
      kafkaReady = true;
      console.log("Kafka connected");
    } catch (err) {
      console.error("Kafka connection failed — location sharing disabled:", err.message);
    }
  })();

  io.on("connection", (socket) => {
    const user = getUserFromRequest(socket.request);
    console.log(`[Socket:${socket.id}]: connected`);

    socket.on("user:location:update", (locationData) => {
      if (!user) {
        socket.emit("server:auth:required");
        return;
      }

      if (!kafkaReady) {
        socket.emit("server:error", { message: "Location sharing is temporarily unavailable." });
        return;
      }

      const { latitude, longitude } = locationData;
      console.log(
        `[Socket:${socket.id}]:user:location:update: location updated to`,
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
              user,
            }),
          },
        ],
      });
    });

    socket.on("disconnect", () => {
      console.log(`[Socket:${socket.id}]: disconnected`);
      io.emit("server:user:disconnected", { id: socket.id });
    });
  });

  app.get("/health", (req, res) => {
    return res.json({ status: "ok" });
  });

  app.get("/api/me", async (req, res) => {
    const user = getUserFromRequest(req);

    return res.json({
      authenticated: Boolean(user),
      user,
      locationIntervalMs: Number(process.env.LOCATION_INTERVAL_MS || 10000),
      oidc: {
        configured: Boolean(getClientId() && getIdpUrl()),
      },
    });
  });

  app.get("/auth/login", async (req, res, next) => {
    try {
      return res.redirect(await buildAuthorizeUrl(req));
    } catch (error) {
      return next(error);
    }
  });

  app.get("/auth/register", async (req, res, next) => {
    try {
      const oidcConfig = await getOidcConfig();
      return res.redirect(await buildAuthorizeUrl(req, oidcConfig.signup_endpoint));
    } catch (error) {
      return next(error);
    }
  });

  app.get("/auth/callback", async (req, res, next) => {
    try {
      const { code, state, error, error_description } = req.query;
      if (error) {
        return res.status(401).send(error_description || error);
      }

      const oidcState = oidcStateStore.get(state);
      oidcStateStore.delete(state);
      if (!code || !oidcState) {
        return res.status(400).send("Invalid OIDC callback state.");
      }

      const tokens = await exchangeCodeForTokens({
        code,
        codeVerifier: oidcState.codeVerifier,
        redirectUri: oidcState.redirectUri,
      });
      const profile = await getOidcProfile(tokens);
      const user = normalizeUser(profile, "oidc");
      createSession(res, user);

      return res.redirect(oidcState.returnTo);
    } catch (error) {
      return next(error);
    }
  });

  app.post("/auth/logout", (req, res) => {
    const sessionId = getSessionId(req);
    if (sessionId) {
      sessionStore.delete(sessionId);
    }
    res.clearCookie(SESSION_COOKIE);
    return res.redirect("/");
  });

  app.use(express.static(publicDir));

  server.listen(PORT, () => {
    console.log(`Server is running on port ${PORT}`);
  });
}

main();
