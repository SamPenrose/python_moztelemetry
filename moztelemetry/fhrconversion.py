'''
Utility function(s) to convert between ping formats.
'''
from collections import defaultdict

V4_SEARCH_KEY_PATH = ('payload', 'keyedHistograms', 'SEARCH_COUNTS')
def search_extractor(v2, v4):
    v4_search_holder = v4
    for k in V4_SEARCH_KEY_PATH:
        v4_search_holder = v4_search_holder.get(k)
        if not v4_search_holder:
            return
    v2_search_holder = v2
    for key, constructor in EXTRACTORS['search']['path']:
        if key not in v2_search_holder:
            v2_search_holder[key] = defaultdict(constructor)
        v2_search_holder = v2_search_holder[key]
    for k in v4_search_holder: # XXX filter
        val = v4_search_holder[k].get('sum')
        if val: # XXX 0 vs None
            v2_search_holder[k] += val


V2_SEARCH_KEY = 'org.mozilla.searches.counts'
EXTRACTORS = {
    'search': {
        'function': search_extractor,
        'path': ((V2_SEARCH_KEY, int),)
    }
}


def KeyError_returns_empty_list(f):
    '''
    Decorator that consolidates key-checking.
    XXX needs logging. The second object in the 3-tuple returned by
    sys.exc_info() is the missing key.

    >>> @KeyError_returns_empty_list
    ... def do():
    ...     x = {}['a']
    ...     return "I won't print"
    ...
    >>> do()
    []
    '''
    def wrapped(*a, **kw):
        try:
            return f(*a, **kw)
        except KeyError:
            return []
    return wrapped


IDLE_DAILY = 'idle-daily'
def ping_is_usable(v4, clientId):
    if v4.get('clientId') != clientId:
        msg = "ClientId conflict: '%s' and '%s'"
        raise ValueError(msg % (v4.get('clientId'), clientId))
    if v4.get('payload', {}).get('info', {}).get(
            'reason', IDLE_DAILY) == IDLE_DAILY:
        return False
    return True


def coalesce_by_date(v4_sequence, dimensions=None):
    '''
    Take an unordered sequence of v4 pings and convert to
    a dictionary keyed by strftime %Y-%m-%d whose values
    are a time-ordered list of pings, including fields via
    the callables in dimensions
    '''
    if not v4_sequence:
        return []
    results = defaultdict(dict) # This will become $data$days
    dimensions = dimensions or EXTRACTORS.keys()
    clientId = v4_sequence[0].get('clientId')
    if not clientId:
        return []

    for v4 in v4_sequence:
        if not ping_is_usable(v4, clientId):
            continue
        try:
            date = v4.get('creationDate', '')[:10]
        except IndexError:
            continue
        if len(date) < 10: # XXX try/except strptime
            continue
        v2 = results[date]
        for dimension in dimensions:
            f = EXTRACTORS[dimension]['function']
            f(v2, v4)
    return results


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
