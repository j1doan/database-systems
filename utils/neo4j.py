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
    """Count nodes per label in the graph."""
    cypher = """
        CALL apoc.meta.stats()
        YIELD labels
        RETURN labels
    """
    # Simpler version that doesn't require APOC:
    labels = ['Subject', 'Session', 'Neuron', 'BrainRegion', 'Stimulus']
    driver = _driver()
    counts = {}
    with driver.session() as s:
        for label in labels:
            result = s.run(f'MATCH (n:{label}) RETURN COUNT(n) AS c')
            counts[label] = result.single()['c']
    driver.close()
    return counts

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