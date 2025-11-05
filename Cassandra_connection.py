from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
import pandas as pd
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def create_cassandra_connection(keyspace='cw', username='', password=''):
    auth_provider = PlainTextAuthProvider(username=username, password=password)
    cluster = Cluster(['10.10.63.40'], auth_provider=auth_provider)
    session = cluster.connect()
    session.set_keyspace(keyspace)
    return session

def upload_absolute_diff(session, csv_path):
    df = pd.read_csv(csv_path)
    session.execute("""
        CREATE TABLE IF NOT EXISTS usedcargstdifference (
            versionid int,
            rootid int,
            absolute_difference int,
            PRIMARY KEY (versionid, rootid)
        );
    """)
    chunk_size = 100
    total = len(df)
    for start in range(0, total, chunk_size):
        chunk = df.iloc[start:start+chunk_size]
        for _, row in chunk.iterrows():
            try:
                versionid = int(str(row['versionid']).replace(',', '').replace('"', '').strip())
                rootid = int(str(row['rootid']).replace(',', '').replace('"', '').strip())
                abs_diff = str(row['absolute_difference']).replace(',', '').replace('"', '').strip()
                if abs_diff == '-' or abs_diff == '':
                    continue
                absolute_difference = int(abs_diff)
                session.execute(
                    "INSERT INTO usedcargstdifference (versionid, rootid, absolute_difference) VALUES (%s, %s, %s)",
                    (versionid, rootid, absolute_difference)
                )
            except Exception:
                continue
        logging.info(f"Uploaded rows {start+1} to {min(start+chunk_size, total)} of {total} to usedcargstdifference table.")

def upload_decay_rate(session, csv_path):
    df = pd.read_csv(csv_path)
    session.execute("""
        CREATE TABLE IF NOT EXISTS usedcargstdecay (
            make_year int,
            Carage int,
            decay_rate int,
            PRIMARY KEY (make_year, Carage)
        );
    """)
    total = len(df)
    for idx, row in enumerate(df.itertuples(), 1):
        try:
            make_year = int(row.make_year)
            carage = int(row.Carage)
            decay_rate = str(row.decay_rate).replace('%', '').strip()
            if decay_rate == '-' or decay_rate == '':
                continue
            decay_rate_int = int(decay_rate)
            session.execute(
                "INSERT INTO usedcargstdecay (make_year, Carage, decay_rate) VALUES (%s, %s, %s)",
                (make_year, carage, decay_rate_int)
            )
        except Exception:
            continue
        if idx % 100 == 0 or idx == total:
            logging.info(f"Uploaded {idx} of {total} rows to usedcargstdecay table.")

def fetch_cassandra_data_for_versions(session, output_csv_path):
    version_ids = [
        14935, 15787, 4882, 17601, 7187, 7184, 13419, 13421, 4321, 13435,
        1677, 6624, 11263, 21316, 2200, 5089, 5089, 6405, 6405, 5294,
        7142, 7142, 13503, 13503, 2425, 6635, 3175, 4848
    ]
    
    query = f"SELECT * FROM cw.usedcarvaluationdata WHERE versionid IN ({', '.join(map(str, version_ids))}) AND applicationid = 1 ALLOW FILTERING;"
    rows = session.execute(query)
    df = pd.DataFrame(rows)
    df.to_csv(output_csv_path, index=False)
    logging.info(f"Fetched {len(df)} rows and saved to {output_csv_path}")

if __name__ == "__main__":
    session = create_cassandra_connection('cw', '', '')
    upload_absolute_diff(session, 'Absolute_diff.csv')
    # upload_decay_rate(session, 'Decay_rate - Decay Rate.csv')
    # fetch_cassandra_data_for_versions(session, 'prod_cassandra_data.csv')
