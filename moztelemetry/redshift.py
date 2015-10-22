'''
we can't actually coalesce by clientid on spark
spark to direct INSERT
  then view?
task becomes pipe to redshift
  update or insert
  maybe insert into temp table
  then conditional update as in effect reduce step
'''
import sys, time
import config

V2_SEARCH_KEY = 'org.mozilla.searches.counts'

SILENT = False
def log(msg):
    if SILENT:
        return
    print msg
    sys.stdout.flush() # Spark can block stdout for ages


class CSVIngester(object):
    '''
    Take a stream of v4 pings and put them into a Redshift table;
    then rework that table into something sensible.

    XXX take a schema.
    '''
    def __init__(self, ping_rdd, mapper):
        self.ping_rdd = ping_rdd
        self.mapper = mapper
        self.csv_rdd = None
        self.csv_path = None

    def do_map(self):
        self.csv_rdd = self.ping_rdd.map(self.mapper).filter(lambda x: x)
        log("RDD mapped")

    def save_as_csv(self, pseudo_dir):
        '''
        Put the return values of extract_for_redshift into an SQL statement
        for our redshift instance.
        '''
        assert self.csv_rdd, "Call do_map() first."
        stamp = int(time.time())
        basename = 'v4-v2-conversion_tempfile-%s.csv' % stamp
        self.csv_path = pseudo_dir + basename
        log("Triggering save to CSV")
        self.csv_rdd.saveAsTextFile(self.csv_path)

db = None
class CSVExporter(object):
    '''
    Move a temp file into Redshift.
    '''
    def __init__(self, csv_path, schema):
        self.csv_path = csv_path
        self.schema = schema
        self.db = config.get_db()

    def export(self):
        create = config.create_temp_table(self.schema)
        self.do_sql(create)
        log('loading %s into %s' % (self.csv_path, self.temp_table))
        load = config.create_load_statement(self.csv_path, self.temp_table)
        self.do_sql(load)

    def do_sql(self, statement):
        try:
            self.cursor.execute(statement)
        except Exception:
            self.db.rollback()
            raise
        else:
            self.db.commmit()


SEARCH_SCHEMA = {
    # dimensions and temp_columns must map 1:1
    'dimensions': [],
    'temp_columns': [],
    'temp_insert': '',
    'temp_to_permanent': '',
    'drop_temp_table': '',
}
