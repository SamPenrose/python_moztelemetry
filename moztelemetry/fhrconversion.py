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
    ...     return x
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
@KeyError_returns_empty_list
def coalesce_by_date(v4_sequence, extractors=None):
    '''
    Take an unordered sequence of v4 pings and convert to
    a dictionary keyed by strftime %Y-%m-%d whose values
    are a time-ordered list of pings, including fields via
    the callables in extractors.

    Be careful not to provoke a KeyError for reasons other than
    data corruption.
    '''
    if not v4_sequence:
        return []
    results = defaultdict(dict) # This is $data$days
    extractors = extractors or EXTRACTORS.keys()
    clientId = v4_sequence[0]['clientId']

    for ping in v4_sequence:
        if ping['clientId'] != clientId:
            msg = "ClientId conflict: '%s' and '%s'"
            raise ValueError(msg % (ping['clientId'], clientId))
        if ping.get('payload', {}).get('info', {}).get(
                'reason', IDLE_DAILY) == IDLE_DAILY:
            continue
        try:
            date = ping['creationDate'][:10]
        except IndexError:
            continue
        v2 = results[date]
        for dimension in extractors:
            f = EXTRACTORS[dimension]['function']
            f(v2, ping)
    return results
