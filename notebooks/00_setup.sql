-- Databricks notebook source
-- Unity Catalog objects for Wikipedia Pulse
CREATE SCHEMA IF NOT EXISTS workspace.wiki_pulse;
CREATE VOLUME IF NOT EXISTS workspace.wiki_pulse.landing;      -- raw hourly files land here
CREATE VOLUME IF NOT EXISTS workspace.wiki_pulse.checkpoints;  -- streaming checkpoints
SHOW VOLUMES IN workspace.wiki_pulse;
