import express from "express";
import type { Server } from "http";
import * as fs from "node:fs";
import * as os from "node:os";
import * as path from "node:path";

import {
  S3Client,
  PutObjectCommand,
  GetObjectCommand,
} from "@aws-sdk/client-s3";
import { getSignedUrl } from "@aws-sdk/s3-request-presigner";

import { env } from "./config/env.js";
import { logger } from "./utils/logger.js";
import { burnCaptions } from "./render/burnCaptions.js";

type TimedCaption = {
  word: string;
  startTime: number;
  endTime: number;
};

type BurnCaptionsRequestBody = {
  videoUrl?: string;
  captions?: TimedCaption[];
  preset?: string;
  jobId?: string | number;
};

function requireEnv(name: string): string {
  const value = process.env[name];
  if (!value) {
    throw new Error(`Missing required env var: ${name}`);
  }
  return value;
}

const r2Client = new S3Client({
  region: "auto",
  endpoint: requireEnv("R2_ENDPOINT"),
  credentials: {
    accessKeyId: requireEnv("R2_ACCESS_KEY_ID"),
    secretAccessKey: requireEnv("R2_SECRET_ACCESS_KEY"),
  },
});

const r2Bucket = requireEnv("R2_BUCKET");

async function uploadVideoToR2(localFilePath: string, objectKey: string): Promise<string> {
  const buffer = fs.readFileSync(localFilePath);

  await r2Client.send(
    new PutObjectCommand({
      Bucket: r2Bucket,
      Key: objectKey,
      Body: buffer,
      ContentType: "video/mp4",
    }),
  );

  const command = new GetObjectCommand({
    Bucket: r2Bucket,
    Key: objectKey,
  });

  // Cast to any to avoid AWS SDK type mismatch issues in this monorepo build
  const signedUrl = await getSignedUrl(
    r2Client as any,
    command as any,
    { expiresIn: 60 * 60 * 24 * 7 },
  );

  return signedUrl;
}

function sanitizeJobId(jobId?: string | number): string {
  return String(jobId ?? Date.now()).replace(/[^a-zA-Z0-9_-]/g, "_");
}

export const createApp = () => {
  const app = express();

  app.use(express.json({ limit: "10mb" }));

  app.get("/health", (_req, res) => {
    res.json({
      status: "ok",
      uptime: process.uptime(),
      version: process.env.npm_package_version,
    });
  });

  app.post("/burn-captions", async (req, res) => {
    const body = (req.body || {}) as BurnCaptionsRequestBody;
    const { videoUrl, captions, preset, jobId } = body;

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

      const message =
        error instanceof Error ? error.message : "Unknown error";

      return res.status(500).json({
        success: false,
        error: message,
      });
    } finally {
      try {
        if (fs.existsSync(outputPath)) {
          fs.unlinkSync(outputPath);
        }
      } catch (_cleanupError) {
        // ignore cleanup failure
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
