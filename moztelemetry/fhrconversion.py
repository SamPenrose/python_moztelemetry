'''
Utility function(s) to convert between ping formats.
'''
from collections import defaultdict

V2_SEARCH_KEY = "org.mozilla.searches.counts"

def DayDict():
    '''
    Represent v2's $data$days blob.
    '''
    def intdict():
        return defaultdict(int)
    def day_factory(): # XXX Add the other keys
        day = {V2_SEARCH_KEY: defaultdict(intdict)}
        return day
    return defaultdict(day_factory)


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
            f(*a, **kw)
        except KeyError:
            return []
    return wrapped


@KeyError_returns_empty_list
def coalesce_by_date(v4_sequence, day_transformer=None):
    '''
    Take an unordered sequence of v4 pings and convert to
    a dictionary keyed by strftime %Y-%m-%d whose values
    are a time-ordered list of pings.

    Given day_transformer, replace the list of pings with
    day_transformer(list_of_pings).

    Be careful not to provoke a KeyError for reasons other than
    data corruption.
    '''
    if not v4_sequence:
        return []
    results = DayDict()
    clientId = v4_sequence[0]['clientId']
    for ping in v4_sequence:
        if ping['clientId'] != clientId:
            msg = "ClientId conflict: '%s' and '%s'"
            raise ValueError(msg % (ping['clientId'], clientId))
        try:
            date = ping['creationDate'][:10]
        except IndexError:
            continue
        for k in v4_search_hists:
            val = 1
            results[date][V2_SEARCH_KEY][k] += val # XXX
    return results
