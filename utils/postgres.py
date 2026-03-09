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
            COUNT(n.neuron_id)  AS n_neurons,
            COUNT(t.trial_id)   AS n_trials
        FROM sessions s
        LEFT JOIN subjects su ON s.subject_id = su.subject_id
        LEFT JOIN neurons  n  ON s.session_id = n.session_id
        LEFT JOIN trials   t  ON s.session_id  = t.session_id
        GROUP BY s.session_id, s.subject_id, su.age, su.sex, su.institution, s.session_date
        ORDER BY s.session_date
    """
    conn = _connect()
    try:
        return pd.read_sql(sql, conn)
    finally:
        conn.close()

def firing_stats(brain_region=None):
    """
    Firing statistics for ALL regions ALL sessions
    """
    sql = """
        SELECT
            brain_region,
            COUNT(*) AS n_neurons,
            AVG(mean_firing_rate) AS avg_firing_rate,
            MIN(mean_firing_rate) AS min_firing_rate,
            MAX(mean_firing_rate) AS max_firing_rate
        FROM neurons
    """
    params = []

    if brain_region is not None:
        sql += " WHERE brain_region = %s"
        params.append(brain_region)

    sql += " GROUP BY brain_region ORDER BY brain_region"

    conn = _connect()
    try:
        return pd.read_sql(sql, conn, params=params)
    finally:
        conn.close()

def firing_stats_hippocampus():
    """
    Firing stats only for Hippocampus neurons
    """
    sql = """
        SELECT
            brain_region,
            COUNT(*) AS n_neurons,
            AVG(mean_firing_rate) AS avg_firing_rate,
            MIN(mean_firing_rate) AS min_firing_rate,
            MAX(mean_firing_rate) AS max_firing_rate
        FROM neurons
        WHERE brain_region ILIKE %s
        GROUP BY brain_region
        ORDER BY brain_region
    """

    conn = _connect()
    try:
        return pd.read_sql(sql, conn, params=['%Hippocampus%'])
    finally:
        conn.close()