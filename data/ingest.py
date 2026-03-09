"""
Stream every NWB session from DANDI 000004 directly into PostgreSQL + Neo4j.
No files are written to disk — HDF5 data is fetched on-demand via HTTP range
requests using remfile, so only the bytes that are actually needed are transferred.
Sessions already present in the DB are skipped.
"""
import numpy as np
import psycopg
import h5py
import remfile
from pynwb import NWBHDF5IO
from dandi.dandiapi import DandiAPIClient
from neo4j import GraphDatabase
import config

DANDISET_ID = '000004'


def _open_stream(asset):
    """Return (NWBHDF5IO, NWBFile) for a DANDI asset — no local file created."""
    s3_url = asset.get_content_url(follow_redirects=1, strip_query=True)
    byte_stream = remfile.File(s3_url)
    h5_file = h5py.File(byte_stream, 'r')
    io = NWBHDF5IO(file=h5_file, load_namespaces=True)
    return io, io.read()


def discover_sessions(dandiset_id=DANDISET_ID):
    """Print all NWB assets available in a DANDI dandiset."""
    with DandiAPIClient() as client:
        assets = [a for a in client.get_dandiset(dandiset_id).get_assets()
                  if a.path.endswith('.nwb')]
    print(f'Dandiset {dandiset_id} — {len(assets)} NWB assets:\n')
    for a in assets:
        print(f'  {a.path}')
    return assets


def ingest_postgres(nwb, asset_path):
    session_id = nwb.identifier
    subject_id = str(nwb.subject.subject_id)
    units_df = nwb.units.to_dataframe()
    trials_df = nwb.trials.to_dataframe() if nwb.trials else None

    # each unit's 'electrodes' cell is a small df of the referenced electrode rows.
    # .iloc[0]['location'] gives the brain region string for that unit.
    regions = {
        i: str(units_df['electrodes'].iloc[i]['location'].iloc[0])
        for i in range(len(units_df))
    }

    with psycopg.connect(config.PG_DSN) as conn, conn.cursor() as cur:
        # idempotent — skip if this session's neurons are already present
        cur.execute('SELECT COUNT(*) FROM neurons WHERE session_id = %s', (session_id,))
        if cur.fetchone()[0] > 0:
            print(f'[Postgres] {session_id} already ingested — skipping.')
            cur.execute(
                'SELECT unit_index, neuron_id FROM neurons WHERE session_id = %s ORDER BY unit_index',
                (session_id,))
            neuron_ids = {r[0]: r[1] for r in cur.fetchall()}
            return neuron_ids, session_id, subject_id, regions

        cur.execute(
            'INSERT INTO subjects (subject_id, age, sex, species, institution) '
            'VALUES (%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING',
            (subject_id, nwb.subject.age, nwb.subject.sex,
             nwb.subject.species, nwb.institution))

        cur.execute(
            'INSERT INTO sessions (session_id, subject_id, session_date, nwb_asset_path) '
            'VALUES (%s,%s,%s,%s) ON CONFLICT DO NOTHING',
            (session_id, subject_id, nwb.session_start_time, asset_path))

        neuron_ids = {}
        for i in range(len(nwb.units)):
            spikes = np.array(nwb.units["spike_times"][i], dtype=float)
            dur = float(spikes[-1] - spikes[0]) if len(spikes) > 1 else 1.0
            iso = units_df['IsolationDist'].iloc[i]
            snr = units_df['SNR'].iloc[i]
            cur.execute(
                'INSERT INTO neurons (session_id, unit_index, brain_region, '
                'n_spikes, mean_firing_rate, spike_times, orig_cluster_id, isolation_dist, snr) '
                'VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s) RETURNING neuron_id',
                (session_id, i, regions[i], len(spikes), len(spikes) / dur, spikes.tolist(),
                 int(units_df['origClusterID'].iloc[i]),
                 None if np.isnan(iso) else float(iso),
                 None if np.isnan(snr) else float(snr)))
            neuron_ids[i] = cur.fetchone()[0]

        if trials_df is not None:
            for j, row in trials_df.iterrows():
                cur.execute(
                    'INSERT INTO trials (session_id, trial_index, stim_phase, '
                    'start_time, stop_time, stim_on_time, stim_off_time, '
                    'delay1_time, delay2_time, stim_category, category_name, '
                    'external_image_file, new_old_labels_recog, response_value, response_time) '
                    'VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)',
                    (session_id, j, row['stim_phase'],
                     row['start_time'], row['stop_time'],
                     row['stim_on_time'], row['stim_off_time'],
                     row['delay1_time'], row['delay2_time'],
                     int(row['stimCategory']), row['category_name'],
                     row['external_image_file'], row['new_old_labels_recog'],
                     row['response_value'], row['response_time']))

        conn.commit()

    n_trials = len(trials_df) if trials_df is not None else 0
    print(f'[Postgres] Ingested {session_id}: {len(neuron_ids)} neurons, {n_trials} trials.')
    return neuron_ids, session_id, subject_id, regions


def ingest_neo4j(nwb, neuron_ids, session_id, subject_id, regions):
    driver = GraphDatabase.driver(config.NEO4J_URI, auth=(config.NEO4J_USER, config.NEO4J_PASS))
    with driver.session() as s:
        # idempotent: remove only this session's nodes, preserving all other sessions
        s.run('MATCH (se:Session {session_id:$sess})-[:HAS_NEURON]->(n:Neuron) DETACH DELETE n',
              sess=session_id)
        s.run('MATCH (se:Session {session_id:$sess}) DETACH DELETE se', sess=session_id)
        s.run('MERGE (su:Subject {subject_id:$id})', id=subject_id)
        s.run('MERGE (se:Session {session_id:$id}) SET se.date=$dt',
              id=session_id, dt=str(nwb.session_start_time))
        s.run('MATCH (su:Subject {subject_id:$subj}),(se:Session {session_id:$sess}) '
              'MERGE (su)-[:HAS_SESSION]->(se)', subj=subject_id, sess=session_id)

        for region in set(regions.values()):
            s.run('MERGE (:BrainRegion {name:$r})', r=region)

        for i, db_id in neuron_ids.items():
            s.run('MERGE (n:Neuron {db_id:$id}) SET n.unit_index=$ui, n.brain_region=$br',
                  id=db_id, ui=i, br=regions[i])
            s.run('MATCH (se:Session {session_id:$sess}),(n:Neuron {db_id:$id}) '
                  'MERGE (se)-[:HAS_NEURON]->(n)', sess=session_id, id=db_id)
            s.run('MATCH (n:Neuron {db_id:$id}),(br:BrainRegion {name:$r}) '
                  'MERGE (n)-[:LOCATED_IN]->(br)', id=db_id, r=regions[i])

    driver.close()
    print(f'[Neo4j]   Ingested {session_id}: {len(neuron_ids)} neurons.')


if __name__ == '__main__':
    # Pre-fetch already-ingested asset paths so we can skip without opening a stream
    with psycopg.connect(config.PG_DSN) as conn, conn.cursor() as cur:
        cur.execute('SELECT nwb_asset_path FROM sessions WHERE nwb_asset_path IS NOT NULL')
        done_paths = {r[0] for r in cur.fetchall()}

    with DandiAPIClient() as client:
        assets = [a for a in client.get_dandiset(DANDISET_ID).get_assets()
                  if a.path.endswith('.nwb')]

    print(f'Dandiset {DANDISET_ID}: {len(assets)} NWB assets found, '
          f'{len(done_paths)} already ingested.\n')

    for asset in assets:
        if asset.path in done_paths:
            print(f'[skip] {asset.path}')
            continue

        print(f'\n--- {asset.path} ---')
        io, nwb = _open_stream(asset)
        try:
            print(f'Session: {nwb.identifier}')
            neuron_ids, session_id, subject_id, regions = ingest_postgres(nwb, asset.path)
            ingest_neo4j(nwb, neuron_ids, session_id, subject_id, regions)
        finally:
            io.close()