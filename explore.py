from iface.reporting import SHARE_RATE_TO_GHASH_PS
from pprint import pprint as pp
from stratum.connection_registry import ConnectionRegistry
from stratum.stats import PeerStats
from iface import dbquery
from mining.interfaces import Interfaces
from poolsrv.posix_time import posix_secs
import time
import datetime
import traceback
import collections
import numpy
import types
import operator
import math

import poolsrv.logger
log = poolsrv.logger.get_logger()


STATS_PERCENTILES = [2, 5, 10, 50, 90, 95, 98]

DATA_POINTS = {
    'connections_count': (True, lambda: PeerStats.get_connected_clients()),
    'dbsink_queue_length': (True, lambda: dbquery.dbsink.get_queue_length()),
    'sessions_count': (True, lambda: _get_sessions_stats()['sessions_count']),
    'hash_rate_short': (True, lambda: _get_sessions_stats()['wsma_ghps_short']),
    'hash_rate_long': (True, lambda: _get_sessions_stats()['wsma_ghps_long']),
}

# A.get_scalar_data_points()

_session_stats_timestamp = time.time() - 2
_session_stats = None


def _get_sessions_stats():
    # Do we have the data computed at most two seconds ago?
    if _session_stats_timestamp > time.time() - 2:
        return _session_stats
    return next(explore(for_sessions, sess_stats))


PREDICATES = {
    'ip': lambda session, ip: session['client_ip'] == ip,
    'user_name': lambda session, user_name: (user_name + '.') in session['authorized'],
    'worker_name': lambda session, worker_name: worker_name in session['authorized'],
    'diff_ge': lambda session, diff: session['SL_difficulty'] > diff,
    'more_workers': lambda session, _: len(session['authorized']) > 1,
    'ghps_ge': lambda session, ghps: Interfaces.share_manager.get_session_short_wsma(session) * SHARE_RATE_TO_GHASH_PS >= ghps
}

_operators_mapping = {
    '<': operator.lt,
    '<=': operator.le,
    '==': operator.eq,
    '!=': operator.ne,
    '>=': operator.ge,
    '>': operator.gt,
}


# Functions added to the callable set by 'explore' in runtime (manhole etc.)
# or any
SPECIAL_FUNCS = {
    # Used for translation from string to callables without actually calling them
    '!': lambda *args: get_callable(*args),
    # Usable for converting lists to tuples since tuples are interpreted as calls
    '(': lambda *args: tuple(args),
    # identity function
    'I': lambda x: x,
}

# A.explore(for_sessions, ('>=', 100))


def _operator_call(oper, pivot, attribute=None):
    def _oc(value):
        if attribute:
            return oper(value[attribute], pivot)
        else:
            return oper(value, pivot)
    return _oc


def explore(*items):
    # Translate the first argument to callable object
    func = get_callable(items[0])
    # Translate
    args = [_explore_argument(item) for item in items[1:]]
    result = func(*tuple(args))
    return result


def _explore_argument(item):
    return explore(*item) if type(item) == tuple else item


def get_callable(func):
    if callable(func):
        return func
    if not type(func) == str:
        raise Exception('%s is not callable' % str(func))

    if _operators_mapping.get(func):
        op = _operators_mapping.get(func)

        def _op_call(*args):
            return _operator_call(op, *args)
        return _op_call

    s = func.split(':')
    if len(s) == 2 and s[0] == 'p':
        return PREDICATES[s[1]]

    # Try to use special functions (added in runtime etc.)
    ret = SPECIAL_FUNCS.get(func)
    if ret:
        return ret

    return globals()[func]


def _chain_filters(source, filters):
    # make a chain from filters
    for f in filters:
        # We expects an iterable object as a source for any filter
        # in the chain
        if not isinstance(source, collections.Iterable):
            raise Exception("Iterable object expected")
        source = f(source)

    return source


def find_connection(id):
    for conn in ConnectionRegistry.iterate():
        try:
            conn = conn()
            if conn.get_session()['session_id'] == id:
                return conn
        except:
            pass
    return None


def _get_sessions_iterator(connections=False):
    for connection_ref in ConnectionRegistry.iterate():
        try:
            connection = connection_ref()
            session = connection.get_session()
            if connections:
                session = session.copy()
                session['connection'] = connection
            yield session
        except:
            # Not connected
            pass


def for_sessions(*filters):
    return _chain_filters(_get_sessions_iterator(False), filters)


def for_conns(*filters):
    return _chain_filters(_get_sessions_iterator(True), filters)


def get_sessions(*filters):
    return list(for_sessions(*filters))


def reverse(iterator):
    complete = list(iterator)
    complete.reverse()
    for i in complete:
        yield i


def where(predicate, *args):
    # Translate predicate from string
    # predicate = get_callable(predicate)

    def _where(iterator):
        for i in iterator:
            if predicate(i, *args):
                yield i
    return _where


def sort_by(key):
    if type(key) == str:
        key_func = lambda val: val[key]
    elif callable(key):
        key_func = key

    def _sorter(iterator):
        complete = list(iterator)
        indices = sorted(range(len(complete)), key=lambda k: key_func(complete[k]))
        for i in indices:
            yield complete[i]
    return _sorter


def narrow(*attribs):
    def _narrow(iterator):
        for i in iterator:
            yield {k: i.get(k, None) for k in attribs}
    return _narrow


def group_by(keys, *filters):
    if callable(keys):
        _get_key = keys
    elif isinstance(keys, (list, tuple)):
        if not len(keys) == 1:
            _get_key = lambda item: tuple([item[k] for k in keys])
        else:
            key = keys[0]
            _get_key = lambda item: item[key]
    else:
        _get_key = lambda item: item[keys]

    def _gb(iterator):
        groups = {}
        # Collect all
        for i in iterator:
            gr_key = _get_key(i)
            group = groups.get(gr_key, None)
            if not group:
                group = []
                groups[gr_key] = group
            group.append(i)

        result = {}
        for gr_key, group in groups.iteritems():
            group_result = _chain_filters(iter(group), filters)
            if isinstance(group_result, types.GeneratorType):
                group_result = list(group_result)

            result[gr_key] = group_result

        yield result
    return _gb


def foreach(*filters):
    def _fe(iterator):
        for i in iterator:
            result = _chain_filters((i,), filters)
            if isinstance(result, types.GeneratorType):
                result = list(result)
            yield result
    return _fe


def project(expression, this=None, pure=True):
    if expression.startswith('g:'):
        expression = expression[2:]
        pure = False

    def _proj(iterator):
        all_items = list(iterator)
        glob = {'__builtins__': None} if pure else globals()
        local_expr = expression
        if len(all_items) > 30:
            local_expr = compile(local_expr, '<explore/runtime>', 'eval')

        for i in all_items:
            try:
                if isinstance(i, dict) and not this:
                    yield eval(local_expr, glob, i)
                else:
                    yield eval(local_expr, glob, {'_': i})
            except:
                log.error(traceback.format_exc())
                yield '<<project:EXCEPTION>>'
    return _proj


def expr(expression, this=None, pure=True):
    if expression.startswith('g:'):
        expression = expression[2:]
        pure = False

    glob = {'__builtins__': None} if pure else globals()

    def _expr(value):
        try:
            if isinstance(value, dict) and not this:
                return eval(expression, glob, value)
            else:
                return eval(expression, glob, {'_': value})
        except:
            log.error(traceback.format_exc())
            return '<<expr:EXCEPTION>>'

    return _expr


def fix_connection(connection, acc):
    if not connection.get_session().get('client_ip'):
        connection.transport.loseConnection()


def fix_connection_dryrun(connection, acc):
    # if not session.get_session().get('client_ip'):
    if not connection.get_session().get('client_ip'):
        pp(connection.get_session())


def sess_convert(iterator):
    for session in iterator:
        yield convert_for_user(session)


def limit(cnt, offset=0):
    def _limit(iterator):
        pos = 0
        high = cnt + offset
        for session in iterator:
            if offset <= pos < high:
                yield session
            pos += 1
    return _limit


def flatten():
    def _flatten(iterator):
        for i in iterator:
            if isinstance(i, dict):
                i = i.iteritems()
            for i2 in i:
                yield i2
    return _flatten


def _unlist(iterator):
    all_ = list(iterator)
    if len(all_) != 1:
        raise Exception('One-element list expected')
    for i in all_[0]:
        yield i


def unlist():
    return _unlist


def count():
    def _count(iterator):
        cnt = 0
        for i in iterator:
            cnt += 1
        yield cnt
    return _count


def stats():
    def _stats(iterator):
        all = list(iterator)
        if len(all) == 0:
            res = {
                'count': 0,
                'min': None, 'max': None, 'sum': None, 'mean': None, 'stddev': None,
                'log_hist': {}
            }
            for perc in STATS_PERCENTILES:
                res['perc%02d' % perc] = None
            yield res
        else:
            percentiles = numpy.percentile(all, STATS_PERCENTILES)
            log_hist = {}
            for val in all:
                if val is None:
                    continue
                if val < 0:
                    bucket = - (2 ** math.ceil(math.log(-val, 2)))
                elif val == 0:
                    bucket = 0
                else:
                    bucket = 2 ** math.ceil(math.log(val, 2))
                if not bucket in log_hist:
                    log_hist[bucket] = 1
                else:
                    log_hist[bucket] += 1

            res = {
                'count': len(all),
                'min': min(all),
                'max': max(all),
                'sum': sum(all),
                'mean': numpy.mean(all),
                'stddev': numpy.std(all),
                'log_hist': log_hist,
            }
            for perc, value in zip(STATS_PERCENTILES, percentiles):
                res['perc%02d' % perc] = value
            yield res

    return _stats


def sess_flatten(iterator):
    for session in iterator:
        template = session.copy()
        del template['SM_worker_stats']

        for key, val in session['SM_worker_stats'].iteritems():
            new_sess = template.copy()
            new_sess['worker_name'] = key
            new_sess.update(val)
            yield new_sess


def convert_for_user(session):
    # Take all authorized workers
    result = {'authorized': session.get('authorized', {}).keys()}

    # Copy some items as they are
    for key in ['session_id', 'client_ip', 'client_sw', 'SL_changes_down', 'SL_changes_up', 'SL_difficulty',
                'unauthorized_submits', 'SL_submission_rate']:
        result[key] = session[key]
    # Transform times to human readable form
    for key in ['SL_difficulty_set_at', 'connected_at', 'disconnected_at', 'subscribed_at']:
        if session[key]:
            # result[key] = time.strftime("%Y-%m-%d_%H:%M:%S", time.gmtime(session[key]))
            result[key] = datetime.datetime.utcfromtimestamp(session[key])
        else:
            result[key] = None

    worker_stats = {}
    for worker, org in session['SM_worker_stats'].iteritems():
        new = {}
        for key in ['invalid_submits', 'old_submits', 'stale_submits', 'valid_shares', 'valid_submits',
                    'worker_name']:
            new[key] = org[key]
        for key in ['authorized_at', 'last_valid_share']:
            if org[key]:
                # new[key] = time.strftime("%Y-%m-%d_%H:%M:%S", time.gmtime(org[key]))
                new[key] = datetime.datetime.utcfromtimestamp(org[key])
            else:
                new[key] = None
        for key in ['wsma_rate_short', 'wsma_rate_long']:
            new[key] = (org[key] * (2 ** 32)) / (1000 * 1000 * 1000)

        worker_stats[worker] = new

    result['SM_worker_stats'] = worker_stats

    return result


def sess_stats(iterator):
    now = posix_secs()

    session_count = connection_time = worker_count = valid_shares = 0
    valid_submits = invalid_submits = stale_submits = old_submits = 0
    has_share = after_share_time = wsma_rate_short = wsma_rate_long = 0
    difficulty_up = difficulty_down = 0
    for session in iterator:
        if not session.get('subscribed_at', None):
            continue

        session_count += 1
        connection_time += (now - int(session['subscribed_at']))
        worker_count += len(session['SM_worker_stats'])
        difficulty_up += session['SL_changes_up']
        difficulty_down += session['SL_changes_down']

        last_share = None
        for ws in session['SM_worker_stats'].itervalues():
            valid_shares += ws['valid_shares']
            valid_submits += ws['valid_submits']
            invalid_submits += ws['invalid_submits']
            stale_submits += ws['stale_submits']
            old_submits += ws['old_submits']

            valid_shares += ws['valid_shares']
            valid_shares += ws['valid_shares']
            wsma_rate_short += ws['wsma_rate_short']
            wsma_rate_long += ws['wsma_rate_long']

            # stats['submits_cum'].append((ws['valid_submits'], ws['stale_submits'], ws['invalid_submits'],
            #                            (ws['wsma_rate_long'] * (2 ** 32)) / (1000 * 1000 * 1000),
            #                            session['SL_difficulty'], session['SL_changes_up'], session['SL_changes_down']))
            if ws['last_valid_share']:
                if last_share is None or ws['last_valid_share'] < last_share:
                    last_share = ws['last_valid_share']

        if last_share:
            has_share += 1
            after_share_time += (now - int(last_share))

    yield {
        'now': now,
        'sessions_count': session_count,
        'connection_time': connection_time,
        'worker_count': worker_count,
        'valid_shares': valid_shares,
        'valid_submits': valid_submits,
        'invalid_submits': invalid_submits,
        'stale_submits': stale_submits,
        'old_submits': old_submits,
        'difficulty_up': difficulty_up,
        'difficulty_down': difficulty_down,
        'wsma_ghps_short': wsma_rate_short * SHARE_RATE_TO_GHASH_PS,
        'wsma_ghps_long': wsma_rate_long * SHARE_RATE_TO_GHASH_PS,
    }
