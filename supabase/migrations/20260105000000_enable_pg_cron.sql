-- Enable pg_cron extension for telemetry scheduling
-- This must run before the telemetry migration
CREATE EXTENSION IF NOT EXISTS pg_cron;
