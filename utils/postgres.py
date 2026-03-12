"""
Helper functions for querying PostgreSQL DB.
"""

import pandas as pd
import psycopg
import config


def _connect():
    return psycopg.connect(config.PG_DSN)


def get_session_summary():
    """
    Summary of all ingested sessions with subject info.
    """
    sql = """
        SELECT
            s.session_id,
            s.subject_id,
            su.age,
            su.sex,
            su.institution,
            s.session_date,
            COUNT(DISTINCT n.neuron_id) AS n_neurons,
            COUNT(DISTINCT t.trial_id)  AS n_trials
        FROM sessions s
        LEFT JOIN subjects su ON s.subject_id = su.subject_id
        LEFT JOIN neurons  n  ON s.session_id = n.session_id
        LEFT JOIN trials   t  ON s.session_id = t.session_id
        GROUP BY s.session_id, s.subject_id, su.age, su.sex, su.institution, s.session_date
        ORDER BY s.session_date
    """
    conn = _connect()
    try:
        return pd.read_sql(sql, conn)
    finally:
        conn.close()


def region_firing_summary():
    """
    Region-level firing summary across all sessions.
    """
    sql = """
            WITH region_base AS (
                SELECT
                    brain_region,
                    session_id,
                    neuron_id,
                    mean_firing_rate
                FROM neurons
            )
            SELECT
                brain_region,
                COUNT(*) AS n_neurons,
                COUNT(DISTINCT session_id) AS n_sessions,
                ROUND(AVG(mean_firing_rate)::numeric, 4) AS avg_firing_rate,
                ROUND(MIN(mean_firing_rate)::numeric, 4) AS min_firing_rate,
                ROUND(MAX(mean_firing_rate)::numeric, 4) AS max_firing_rate
            FROM region_base
            GROUP BY brain_region
            ORDER BY avg_firing_rate DESC, max_firing_rate DESC, min_firing_rate DESC;
    """
    conn = _connect()
    try:
        return pd.read_sql(sql, conn)
    finally:
        conn.close()


def region_ranked_neurons(top_n=5):
    """
    Top neurons by firing rate within each brain region using window functions.
    """
    sql = """
        WITH ranked AS (
            SELECT
                brain_region,
                session_id,
                neuron_id,
                mean_firing_rate,
                ROW_NUMBER() OVER (
                    PARTITION BY brain_region
                    ORDER BY mean_firing_rate DESC, neuron_id
                ) AS region_rank
            FROM neurons
        )
        SELECT
            brain_region,
            session_id,
            neuron_id,
            ROUND(mean_firing_rate::numeric, 4) AS mean_firing_rate,
            region_rank
        FROM ranked
        WHERE region_rank <= %s
        ORDER BY brain_region, region_rank
    """
    conn = _connect()
    try:
        return pd.read_sql(sql, conn, params=[top_n])
    finally:
        conn.close()


def region_spike_distribution():
    """
    Expand spike_times with UNNEST and summarize spike-time distributions by region.
    """
    sql = """
        WITH expanded AS (
            SELECT
                n.brain_region,
                n.session_id,
                n.neuron_id,
                UNNEST(n.spike_times) AS spike_time
            FROM neurons n
        )
        SELECT
            brain_region,
            COUNT(*) AS total_spikes,
            COUNT(DISTINCT neuron_id) AS n_neurons,
            ROUND(AVG(spike_time)::numeric, 4) AS avg_spike_time,
            ROUND(MIN(spike_time)::numeric, 4) AS first_spike_time,
            ROUND(MAX(spike_time)::numeric, 4) AS last_spike_time
        FROM expanded
        GROUP BY brain_region
        ORDER BY brain_region
    """
    conn = _connect()
    try:
        return pd.read_sql(sql, conn)
    finally:
        conn.close()


def region_session_zscores():
    """
    Compare each session's region-level average firing rate against the region mean.
    """
    sql = """
        WITH session_region AS (
            SELECT
                brain_region,
                session_id,
                AVG(mean_firing_rate) AS session_avg_rate
            FROM neurons
            GROUP BY brain_region, session_id
        ),
        scored AS (
            SELECT
                brain_region,
                session_id,
                session_avg_rate,
                AVG(session_avg_rate) OVER (PARTITION BY brain_region) AS region_mean,
                STDDEV_POP(session_avg_rate) OVER (PARTITION BY brain_region) AS region_std
            FROM session_region
        )
        SELECT
            brain_region,
            session_id,
            ROUND(session_avg_rate::numeric, 4) AS session_avg_rate,
            ROUND(region_mean::numeric, 4) AS region_mean,
            ROUND(
                CASE
                    WHEN region_std = 0 THEN 0
                    ELSE (session_avg_rate - region_mean) / region_std
                END::numeric,
                4
            ) AS z_score_within_region
        FROM scored
        ORDER BY brain_region, z_score_within_region DESC, session_id
    """
    conn = _connect()
    try:
        return pd.read_sql(sql, conn)
    finally:
        conn.close()