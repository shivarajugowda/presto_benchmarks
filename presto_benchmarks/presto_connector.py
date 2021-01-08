import presto

class PrestoConnection:
    def __init__(self, cfg):
        self.cfg = cfg

    def __enter__(self):
        self.conn = presto.dbapi.connect(
            host=self.cfg['host'],
            port=self.cfg['port'],
            user=self.cfg['user']
        )

        return self

    def __exit__(self, type, value, traceback):
        if self.conn:
            self.conn.close()

    def gettables(self, catalog, schema):
        sql = 'SELECT table_name FROM {}.information_schema.tables where table_catalog = \'{}\' and table_schema = \'{}\''.format(catalog, catalog, schema)
        rows = self.run_query(sql)
        return sum(rows, [])  # flatten 2d to 1d

    def getcolumns(self, catalog, schema, table):
        sql = 'SELECT column_name FROM {}.information_schema.columns where table_catalog = \'{}\' and table_schema = \'{}\' and table_name = \'{}\' '.format(catalog, catalog, schema, table)
        rows = self.run_query(sql)
        return sum(rows, [])  # flatten 2d to 1d

    def createschema(self, catalog, schema):
        self.run_query('CREATE SCHEMA IF NOT EXISTS "{}"."{}" '.format(catalog, schema))

    def droptable(self, catalog, schema, tbl):
        self.run_query('DROP TABLE IF EXISTS "{}"."{}"."{}" '.format(catalog, schema, tbl))

    def analyzetable(self, catalog, schema, tbl):
        self.run_query('ANALYZE "{}"."{}"."{}" '.format(catalog, schema, tbl))

    def copytable(self, tbl, src_catalog, src_schema, cols, dest_catalog, dest_schema, fileformat):
        formatclause = ""
        if fileformat:
            formatclause = 'WITH (format = \'{}\')'.format(fileformat)
        self.run_query('CREATE TABLE "{}"."{}"."{}" {} AS SELECT {} FROM "{}"."{}"."{}" '.format(
            dest_catalog, dest_schema, tbl, formatclause, cols, src_catalog, src_schema, tbl))

    def getclusterinfo(self):
        rows = self.run_query('SELECT count(*) as num_workers, max(node_version) as presto_version FROM system.runtime.nodes')
        output = tuple(rows[0])
        try:
            rows = self.run_query('SELECT vmversion as jdk_version, inputarguments as jvm_args FROM jmx.current."java.lang:type=runtime"')
        except:
            rows = ("unknown", "unknown")
        output = output + tuple(rows[0])
        return output

    def run_query(self, query):
        #print('Executing : {}'.format(query))
        cur = self.conn.cursor()
        cur.execute(query)
        rows = cur.fetchall()
        cur.close()
        return rows

    def close(self):
        if self.conn:
            self.conn.close()



