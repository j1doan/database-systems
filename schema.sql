-- schema.sql
-- Run this against your PostgreSQL database BEFORE running ingest.py:
--   psql $PG_DSN -f schema.sql

DROP TABLE IF EXISTS subjects;

CREATE TABLE IF NOT EXISTS subjects (
    subject_id  TEXT PRIMARY KEY,
    age         TEXT,
    sex         TEXT,
    species     TEXT,
    institution TEXT
);

DROP TABLE IF EXISTS sessions;

CREATE TABLE IF NOT EXISTS sessions (
    session_id     TEXT PRIMARY KEY,      -- nwb.identifier, e.g. "H09_5"
    subject_id     TEXT REFERENCES subjects (subject_id),
    session_date   TIMESTAMPTZ,
    nwb_asset_path TEXT
);

DROP TABLE IF EXISTS neurons;

CREATE TABLE IF NOT EXISTS neurons (
    neuron_id        SERIAL PRIMARY KEY,
    session_id       TEXT REFERENCES sessions (session_id),
    unit_index       INT,
    brain_region     TEXT,
    n_spikes         INT,
    mean_firing_rate FLOAT,              -- Hz (spikes / recording duration)
    spike_times      FLOAT[],            -- all spike timestamps (seconds)
    orig_cluster_id  INT,               -- origClusterID from NWB units table
    isolation_dist   FLOAT,             -- IsolationDist: spike sorting quality
    snr              FLOAT              -- SNR: signal-to-noise ratio
);

DROP TABLE IF EXISTS trials;

CREATE TABLE IF NOT EXISTS trials (
    trial_id              SERIAL PRIMARY KEY,
    session_id            TEXT REFERENCES sessions (session_id),
    trial_index           INT,
    stim_phase            TEXT,          -- 'learn' (encoding) or 'recognition'
    start_time            FLOAT,
    stop_time             FLOAT,
    stim_on_time          FLOAT,         -- stimulus onset time
    stim_off_time         FLOAT,         -- stimulus offset time
    delay1_time           FLOAT,
    delay2_time           FLOAT,
    stim_category         INT,           -- numeric category ID (stimCategory)
    category_name         TEXT,          -- e.g. 'smallAnimal', 'face'
    external_image_file   TEXT,          -- relative path to stimulus image
    new_old_labels_recog  TEXT,          -- 'NA' (learn) | 'new' | 'old'
    response_value        FLOAT,         -- subject's button response
    response_time         FLOAT          -- time of button press
);
