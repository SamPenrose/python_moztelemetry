try:
    import ujson as json
except ImportError:
    import json
import subprocess
from collections import OrderedDict
import psycopg2

V4_DATE_FORMAT = '%Y%m%d'
V4_RELEASE_DATE = '20150922'
S3_LOCATION = 's3n://net-mozaws-prod-us-west-2-pipeline-analysis/' \
              'spenrose/v4_release_qa/'

ES_MAIN_PING_KEYS = [
    'Hostname', 'Timestamp', 'Type', 'activityTimestamp', 'app', 'bing',
    'buildId', 'channel', 'clientId', 'country', 'default', 'docType',
    'documentId', 'google', 'hours', 'os', 'osVersion', 'other',
    'pluginHangs', 'reason', 'submissionDate', 'vendor', 'version', 'yahoo']
ES_CRASH_PING_KEYS = [
    'Hostname', 'Timestamp', 'Type', 'activityTimestamp', 'app', 'buildId',
    'channel', 'clientId', 'country', 'default', 'docType', 'documentId',
    'os', 'osVersion', 'submissionDate', 'vendor', 'version']

SMALLINT = 'smallint'
REAL = 'real'
VARCHAR32 = 'varchar(32)'
VARCHAR256 = 'varchar(256)'
DATE = 'date'

# Schema for a row holding just enough of a v4 ping to identify its searches.
V4_SEARCH_PING_SCHEMA = OrderedDict([
    ('clientid', VARCHAR32),
    ('active_date', DATE),
    ('yahoo', SMALLINT),
    ('google', SMALLINT),
    ('bing', SMALLINT),
    ('other', SMALLINT),
])

SEARCH_PROVIDERS = [
    'yahoo',
    'google',
    'bing'
]
for provider in SEARCH_PROVIDERS:
    assert provider in V4_SEARCH_PING_SCHEMA, "Fix config.py"

_db = None
def get_db():
    global _db
    if not _db:
        _db = psycopg2.connect(
            host='moz-metrics-v2-v4-pairs.cfvijmj2d97c.'\
            'us-west-2.redshift.amazonaws.com',
            port=5439,
            dbname='dev',
            user='masteruser',
            password='R5L33n9I2wNl')
    return _db


CREATE_TEMP_TABLE = """\
  create table ingest_csv_temp_%s (
    %s
  )
"""


def make_row_declarations(schema):
    statements = ['%s %s' % row for row in schema]
    return ',\n'.join(statements)


def create_temp_table(schema, stamp):
    return CREATE_TEMP_TABLE % (stamp, make_row_declarations(schema))


def get_creds():
    cmd = ['curl', 'http://169.254.169.254/latest/meta-data/iam/security-credentials/telemetry-spark-emr']
    cred_string = subprocess.check_output(cmd)
    return json.loads(cred_string)

LOAD_CSV_INTO_TEMP = """\
  copy %(TempTableName)s
  from %(CSVpath)s
  credentials aws_access_key_id=%(AccessKeyId)s;aws_secret_access_key=%(SecretAccessKey)s;token=%(Token)s
"""

def create_load_statement(csv_path, temp_table):
    params = get_creds()
    params['TempTableName'] = temp_table
    params['CSVpath'] = csv_path
    return LOAD_CSV_INTO_TEMP % params
