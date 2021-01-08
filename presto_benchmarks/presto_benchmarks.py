# presto_benchmarks.py
import glob
from datetime import datetime
import yaml, sys
from pathlib import Path
from .presto_connector import PrestoConnection
from .metrics_store import MetricsConnection
from functools import partial
import multiprocessing, re
import pandas as pd
from pandas._testing import assert_frame_equal


def stampQuery(pb, sql, catalog, schema):
    for r in (("${database}", catalog), ("${schema}", schema), ("${prefix}", ''), (";", '')):
        sql = sql.replace(*r)

    if pb.cfg['run']['handledateasvarchar']:
        sql = re.sub('([\\w.]+date)', r'CAST(\1 AS DATE)', sql)

    if pb.cfg['run']['distributed_join_sort']:
        sql = "-- set session join_distribution_type = 'PARTITIONED' \n" + sql
        sql = "-- set session distributed_sort = 'true' \n" + sql

    return sql

def runBenchmark(task):
    pb, seq, queryfile = task
    query = Path(queryfile).stem
    with open(queryfile, "r") as myfile:
        sql_template = myfile.read()

    with PrestoConnection(pb.cfg['presto']) as presto:
        sql = stampQuery(pb, sql_template, pb.dest_catalog, pb.dest_schema)
        try:
            starttime = datetime.now()
            rows = presto.run_query(sql)
            endtime = datetime.now()
            print(' Run {} Time Taken : {}  --- NumRows : {} '.format(query,  (endtime - starttime), str(len(rows)) ))
            with MetricsConnection(pb.cfg['metricsstore']) as metrics:
                metrics.addentry(pb.id, "b", query, seq, starttime, endtime)
        except Exception as e:
            print(' Run {}  --- Exception {} '.format(query, str(e)))


def verifyResults(task):
    pb, seq, queryfile = task
    query = Path(queryfile).stem
    with open(queryfile, "r") as myfile:
        sql_template = myfile.read()

    with PrestoConnection(pb.cfg['presto']) as presto:
        starttime = datetime.now()
        sql = stampQuery(pb, sql_template, pb.dest_catalog, pb.dest_schema)
        df_rows = pd.DataFrame(presto.run_query(sql))

        sql = stampQuery(pb, sql_template, pb.src_catalog, pb.src_schema)
        control_rows = pd.DataFrame(presto.run_query(sql))
        endtime = datetime.now()
        print(' Verify {} Time Taken : {}  --- {}  {}'.format(query,  (endtime - starttime), str(len(df_rows)),  str(len(control_rows))))

        try:
            assert_frame_equal(df_rows, control_rows)
        except:
            print(' Verification for {} Failed \n ---- Expected : {}  \n --- Actual : {} ', queryfile,  str(control_rows), str(df_rows))

def createTable(pb, tbl):
    with PrestoConnection(pb.cfg['presto']) as presto:
        with MetricsConnection(pb.cfg['metricsstore']) as metrics:
            presto.droptable(pb.dest_catalog, pb.dest_schema, tbl)
            cols = "*" # default all columns
            if pb.cfg['run']['handledateasvarchar']:
                columns = presto.getcolumns(pb.src_catalog, pb.src_schema, tbl)
                modified = False
                for x in range(len(columns)):
                    if (columns[x].endswith('date') ):
                        columns[x] = "CAST ({} AS VARCHAR) AS {}".format(columns[x], columns[x])
                        modified = True
                if modified:
                    cols = ', '.join(columns)

            starttime = datetime.now()
            presto.copytable(tbl, pb.src_catalog, pb.src_schema, cols, pb.dest_catalog, pb.dest_schema,
                             pb.fileformat);

            #presto.analyzetable(pb.src_catalog, pb.src_schema, tbl)
            endtime = datetime.now()
            print("Recreating table : ", tbl, " Time Taken : %s  ---" % (endtime - starttime))
            metrics.addentry(pb.id, "p", tbl, 0, starttime, endtime)


class PrestoBenchmarks:
    def __init__(self, configfile, description):
        with open(configfile, "r") as ymlfile:
            self.cfg = yaml.load(ymlfile, Loader=yaml.FullLoader)

        self.description = description
        self.src_catalog = self.cfg['run']['sourcecatalog']
        self.src_schema  = self.cfg['run']['size']

        self.dest_catalog = self.cfg['run']['connector']
        self.dest_schema  = '{}_{}_{}'.format( self.cfg['run']['type'],  self.cfg['run']['fileformat'], self.cfg['run']['size'])

        self.fileformat = self.cfg['run']['fileformat']
        self.id = -1

        self.concurrency = self.cfg['run']['concurrency']
        self.runs = self.cfg['run']['runs']

    def setup(self):
        with MetricsConnection(self.cfg['metricsstore']) as metrics:
            with PrestoConnection(self.cfg['presto']) as presto:
                clusterinfo = presto.getclusterinfo()
                self.id = metrics.setup(self.cfg['run']['type'], self.cfg['run']['size'], self.cfg['run']['concurrency'], self.fileformat, self.description, clusterinfo)
                print("Starting Benchmark : ID = %d , Size = %s, Concurrency = %s" % ( self.id, self.cfg['run']['size'], self.concurrency) )

        if self.cfg['presto']['recreatetables']:
            starttime = datetime.now()
            with PrestoConnection(self.cfg['presto']) as presto:
                tables = presto.gettables(self.src_catalog, self.src_schema)
                presto.createschema(self.dest_catalog, self.dest_schema)

            pool = multiprocessing.Pool(self.concurrency)
            func = partial(createTable, self)
            pool.map(func, tables)
            pool.close()
            pool.join()
            print("Recreating table done. Time taken : %s  " % (datetime.now() - starttime))

    def verifyResults(self):
        if not self.cfg['run']['verifyresults'] :
            return

        print('Verifying tests : ')
        path = Path( self.cfg['benchmarks']['location'], self.cfg['run']['type'])
        if not path.exists():
            sys.exit("ERROR: Benchmark path does not exist : " + str(path))

        queries = glob.glob(str(path) + '/*.sql')
        queries.sort()

        starttime = datetime.now()
        tasks = []
        for query in queries:
            tasks.append((self, 0, query))

        pool = multiprocessing.Pool(self.concurrency)
        pool.map(verifyResults, tasks)
        pool.close()
        pool.join()

        print("Verifying results completed. Time taken : %s  " % (datetime.now() - starttime))

    def execute(self):
        print('Running benchmark : ')
        path = Path( self.cfg['benchmarks']['location'], self.cfg['run']['type'])
        if not path.exists():
            sys.exit("ERROR: Benchmark path does not exist : " + str(path))

        queries = glob.glob(str(path) + '/*.sql')
        queries.sort()

        starttime = datetime.now()
        tasks = []
        for x in range(0, self.runs):
            for query in queries:
                tasks.append((self, x, query))

        pool = multiprocessing.Pool(self.concurrency)
        pool.map(runBenchmark, tasks)
        pool.close()
        pool.join()

        print("Running benchmarks completed. Time taken : %s  " % (datetime.now() - starttime))

    @staticmethod
    def run(configfile, description):
        runner = PrestoBenchmarks(configfile, description);
        runner.setup()
        runner.verifyResults()
        runner.execute()
