"""
Helper functions for querying Neo4j graph DB.
"""

import pandas as pd
from neo4j import GraphDatabase
import config


def _driver():
    return GraphDatabase.driver(
        config.NEO4J_URI, auth=(config.NEO4J_USER, config.NEO4J_PASS)
    )


def get_graph_summary():
    """Return node counts grouped by label using a simple Cypher query.

    Returns a list of dicts: [{'labels': [...], 'count': n}, ...]
    """
    cypher = """
    MATCH (n)
    RETURN labels(n) AS labels, count(n) AS count
    """
    driver = _driver()
    results = []
    with driver.session() as s:
        for record in s.run(cypher):
            r = record['r']
            results.append(dict(r))
    driver.close()
    return results

def get_brain_regions():
    """
    Get all brain regions as a list of dicts.
    """
    cypher = """
    MATCH (r:BrainRegion)
    RETURN r
    """
    driver = _driver()
    results = []
    with driver.session() as s:
        for record in s.run(cypher):
            r = record['r']
            results.append(dict(r))
    driver.close()
    return results