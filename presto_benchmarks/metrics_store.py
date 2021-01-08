import psycopg2
from datetime import datetime

class MetricsConnection:
    def __init__(self, cfg):
        self.enabled = cfg['enabled']
        self.recreatetables = cfg['recreatetables']
        self.cfg = cfg

    def __enter__(self):
        if self.enabled :
            self.conn = psycopg2.connect( user=self.cfg['user'],
                                          password=self.cfg['password'],
                                          host=self.cfg['host'],
                                          port=self.cfg['port'],
                                          database=self.cfg['database'])
            self.conn.set_session(autocommit=True)
        return self

    def __exit__(self, type, value, traceback):
        if self.enabled :
            self.conn.close()

    def populate(self):
        cur = self.conn.cursor()
        cur.execute('DROP TABLE IF EXISTS benchmarks CASCADE')
        cur.execute('DROP TABLE IF EXISTS benchmark_details CASCADE')

        sql = ''' CREATE TABLE benchmarks (  
                  id BIGSERIAL PRIMARY KEY,
                  starttime TIMESTAMP WITH TIME ZONE,
                  endtime TIMESTAMP WITH TIME ZONE,
                  type VARCHAR(50),
                  size VARCHAR(50),
                  concurrency int, 
                  fileformat VARCHAR(50),
                  numnodes int, 
                  presto_version VARCHAR(50),
                  jdk_version VARCHAR(50),
                  vm_args VARCHAR,
                  description VARCHAR
                ); '''
        cur.execute(sql)

        sql = ''' CREATE TABLE benchmark_details (  
                  id BIGINT REFERENCES benchmarks(id),
                  type CHAR,
                  name VARCHAR(50),
                  seq int,
                  starttime TIMESTAMP WITH TIME ZONE,
                  endtime  TIMESTAMP WITH TIME ZONE, 
                  error_msg VARCHAR(50),
                  PRIMARY KEY (id, type, name, seq)
                ); '''
        cur.execute(sql)

        cur.close()

    def setup(self, type, size, concurrency, fileformat, description, clusterinfo):
        if not self.enabled:
            return -1

        if self.recreatetables:
            self.populate()

        cur = self.conn.cursor()
        cur.execute('''
                    INSERT INTO benchmarks (starttime, type, size, concurrency, fileformat, numnodes, presto_version, jdk_version, vm_args, description)
                    VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s) RETURNING id ''',
                    (datetime.now(), type, size, concurrency, fileformat, clusterinfo[0], clusterinfo[1], clusterinfo[2], clusterinfo[3], description))
        rows = cur.fetchall()
        cur.close()
        id = rows[0][0]
        print("ID = ", id)
        return id

    def addentry(self, id, type, metricname, seq, starttime, endtime):
        if not self.enabled:
            return
        cur = self.conn.cursor()
        cur.execute("INSERT INTO benchmark_details (id, type, name, seq, starttime, endtime) VALUES(%s, %s, %s, %s, %s, %s)",
                    (id, type, metricname, seq, starttime, endtime))
        cur.close()

    def close(self):
        if self.conn:
            self.conn.close()
