import express from "express";
import type { Server } from "http";
import type { Caption } from "captions.js";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";

import {
  S3Client,
  PutObjectCommand,
  GetObjectCommand,
} from "@aws-sdk/client-s3";
import { getSignedUrl } from "@aws-sdk/s3-request-presigner";

import { env } from "./config/env.js";
import { logger } from "./utils/logger.js";
import { burnCaptions } from "./render/burnCaptions.js";

type BurnCaptionsRequestBody = {
  videoUrl: string;
  captions: Caption[];
  preset: string;
  jobId?: string | number;
};

const requiredR2Env = [
  "R2_ENDPOINT",
  "R2_ACCESS_KEY_ID",
  "R2_SECRET_ACCESS_KEY",
  "R2_BUCKET",
] as const;

function assertR2Env() {
  const missing = requiredR2Env.filter((key) => !process.env[key]);
  if (missing.length > 0) {
    throw new Error(`Missing R2 env vars: ${missing.join(", ")}`);
  }
}

function getR2Client() {
  assertR2Env();

  return new S3Client({
    region: "auto",
    endpoint: process.env.R2_ENDPOINT!,
    credentials: {
      accessKeyId: process.env.R2_ACCESS_KEY_ID!,
      secretAccessKey: process.env.R2_SECRET_ACCESS_KEY!,
    },
  });
}

async function uploadVideoToR2(localFilePath: string, objectKey: string) {
  const client = getR2Client();
  const bucket = process.env.R2_BUCKET!;

  const buffer = fs.readFileSync(localFilePath);

  await client.send(
    new PutObjectCommand({
      Bucket: bucket,
      Key: objectKey,
      Body: buffer,
      ContentType: "video/mp4",
    }),
  );

  if (process.env.R2_PUBLIC_URL) {
    return `${process.env.R2_PUBLIC_URL.replace(/\/$/, "")}/${objectKey}`;
  }

  return getSignedUrl(
    client,
    new GetObjectCommand({
      Bucket: bucket,
      Key: objectKey,
    }),
    { expiresIn: 60 * 60 * 24 * 7 },
  );
}

function sanitizeJobId(jobId?: string | number) {
  return String(jobId ?? Date.now()).replace(/[^a-zA-Z0-9_-]/g, "_");
}

export const createApp = () => {
  const app = express();

  // Default express limit is too small for long caption arrays
  app.use(express.json({ limit: "10mb" }));

  app.get("/health", (_req, res) => {
    res.json({
      status: "ok",
      uptime: process.uptime(),
      version: process.env.npm_package_version,
    });
  });

  // New route that matches your worker contract
  app.post("/burn-captions", async (req, res) => {
    const body = req.body as BurnCaptionsRequestBody;

    const { videoUrl, captions, preset, jobId } = body ?? {};

    if (!videoUrl || typeof videoUrl !== "string") {
      return res.status(400).json({
        success: false,
        error: "videoUrl is required",
      });
    }

    if (!Array.isArray(captions) || captions.length === 0) {
      return res.status(400).json({
        success: false,
        error: "captions must be a non-empty array",
      });
    }

    if (!preset || typeof preset !== "string") {
      return res.status(400).json({
        success: false,
        error: "preset is required",
      });
    }

    const safeJobId = sanitizeJobId(jobId);
    const timestamp = Date.now();

    const outputPath = path.join(
      os.tmpdir(),
      `${safeJobId}-captioned-${timestamp}.mp4`,
    );

    const objectKey = `jobs/${safeJobId}/final-captioned-${timestamp}.mp4`;

    try {
      logger.info(
        {
          jobId: safeJobId,
          preset,
          captionsCount: captions.length,
          videoUrl,
        },
        "Starting caption burn",
      );

      await burnCaptions({
        video: videoUrl,
        captions: JSON.stringify(captions),
        output: outputPath,
        preset,
      });

      const uploadedUrl = await uploadVideoToR2(outputPath, objectKey);

      logger.info(
        {
          jobId: safeJobId,
          objectKey,
          uploadedUrl,
        },
        "Caption burn completed",
      );

      return res.json({
        success: true,
        videoUrl: uploadedUrl,
        key: objectKey,
      });
    } catch (error) {
      logger.error(
        {
          err: error,
          jobId: safeJobId,
        },
        "Caption burn failed",
      );

      return res.status(500).json({
        success: false,
        error: error instanceof Error ? error.message : "Unknown error",
      });
    } finally {
      try {
        if (fs.existsSync(outputPath)) {
          fs.unlinkSync(outputPath);
        }
      } catch (cleanupError) {
        logger.warn({ err: cleanupError, outputPath }, "Failed to clean temp file");
      }
    }
  });

  return app;
};

export const startServer = (): Server => {
  const app = createApp();
  const server = app.listen(env.PORT, "0.0.0.0", () => {
    logger.info({ port: env.PORT }, "Server started");
  });

  const terminationSignals: NodeJS.Signals[] = ["SIGTERM", "SIGINT"];

  terminationSignals.forEach((signal) => {
    process.on(signal, () => {
      logger.info({ signal }, "Received shutdown signal");
      server.close(() => {
        logger.info("HTTP server closed");
        process.exit(0);
      });
    });
  });

  return server;
};
