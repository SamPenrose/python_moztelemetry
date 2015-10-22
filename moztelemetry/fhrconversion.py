'''
Convert a subset of a V4 ping to a subset of a V2 ping.

1) Define the subset we want.
2) map to each ping:
  - a validator that sees if we can use the ping
  - an ordered set of extractors
    + each of which returns a value for ','.join()
3) filter() and dump to .csv
4) write to temp table
5) join/coalesce inside redshift
'''
import datetime as DT
import config
import redshift as RED


def get_search_provider(name): # XXX
    name = name.lower()
    for provider in config.SEARCH_PROVIDERS:
        if provider in name:
            return provider
    return 'other'


IDLE_DAILY = 'idle-daily'
def ping_is_usable(v4):
    if len(v4.get('clientId', '')) != 32: # UUID
        # msg = "Bad clientId: '%s'"
        return False
    if v4.get('payload', {}).get('info', {}).get(
            'reason', IDLE_DAILY) == IDLE_DAILY:
        return False
    if len(v4.get('creationDate')[:10]) != 10:
        return False
    return True


V4_SEARCH_KEY_PATH = ('payload', 'keyedHistograms', 'SEARCH_COUNTS')
def search_extractor(v4):
    if not ping_is_usable(v4):
        return ''

    # Create a CSV row with sensible defaults.
    row = config.V4_SEARCH_PING_SCHEMA.copy() # a flat OrderedDict
    for k in row:
        row[k] = 0
    row['clientId'] = v4['clientId']
    row['active_date'] = v4['creationDate'][:10]

    v4_search_holder = v4
    for k in V4_SEARCH_KEY_PATH:
        v4_search_holder = v4_search_holder.get(k)
        if not v4_search_holder:
            return
    for k in v4_search_holder:
        val = v4_search_holder[k].get('sum')
        name = get_search_provider(k)
        row[name] += val
    return ','.join(row.values())


def make_ES_filter(key2whitelist, falseValue=None):
    '''
    Return a function which will test an "Exectuve Summary" blob returned
    by spark.get_records() with the (key, whitelist) pairs, returning the
    blob if it passes otherwise returning falseValue.

    >>> f = make_ES_filter({'a': 1, 'b': [2, 3]}, [])
    >>> f({'a': 1, 'b': 2})
    {'a': 1, 'b': 2}
    >>> f({'a': 1, 'b': 3, 'c': object()}) # extra keys are ignored
    {'a': 1, 'c': <object object at 0x108725fe0>, 'b': 3}
    >>> f({'a': 1, 'b': 4})
    []
    >>> f({'a': 1, 'b': [2, 3]})
    []
    >>> f({'b': 3})
    []
    '''
    assert bool(falseValue) is False # Necessary to make this a filter
    sentinel = object() # Never equal to a whitelist value
    def filterer(d): # Don't mask builtin filter()
        d = d.get('meta', d)
        for key, whitelist in key2whitelist.items():
            if not isinstance(whitelist, list) or \
               isinstance(whitelist, tuple): # This is a bit fragile
                whitelist = [whitelist]
            passes = False
            for value in whitelist:
                if d.get(key, sentinel) == value:
                    passes = True
                    break
            if not passes:
                return falseValue
        return d
    return filterer


def get_executive_summary(**kw):
    import spark
    return spark.get_records(sc, 'telemetry-executive-summary', **kw)


def tomorrow_string():
    date = (DT.datetime.now() + DT.timedelta(1)).date()
    return date.strftime(config.V4_DATE_FORMAT)


def convert(sc, fraction=0.1, channel='release', version=None):
    '''
    fraction: usual moztelemetry fraction of pings.
    '''
    import spark
    ping_rdd = spark.get_pings(sc, fraction=fraction, channel=channel,
                               version=version, doc_type='main')
    # XXX bundle extractor and schema
    ingester = RED.CSVIngester(ping_rdd, search_extractor)
    ingester.do_map()
    ingester.save_as_csv(config.S3_LOCATION)
    exporter = RED.CSVExporter(ingester, config.V4_SEARCH_PING_SCHEMA) # XXX
    exporter.export()
