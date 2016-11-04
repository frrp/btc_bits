import config_location
import random
import datetime
import os
import socket

INSTANCE = int(os.environ.get('INSTANCE', 1))
HOST = socket.gethostname()

STRATUM_UNIQUE_ID = "%s-%d" % (HOST, INSTANCE)


TEMPORARY__SCORE_BY_TIME_ONLY = False
TEMPORARY__DATABASE_USE_BLOCK_TEMPALATE = True


# ******************** DEBUG SETTINGS ***************

# Enable some verbose debug (logging requests and responses).
DEBUG = config_location.DEBUG

if config_location.__dict__.get('___DANGEROUS_FEATURES_ENABLED___', False):
    # User shares that don't meet difficulty criteria are accepted if set to True
    ACCEPT_SHARES_ABOVE_TARGET = config_location.__dict__.get('ACCEPT_SHARES_ABOVE_TARGET', False)

    # Share's nonce interpreted as a request for a block! If a nonce matches this HEX value then
    # a hash of the submit is ignored and a block is generated.
    # If set to None, the functionality is disabled.
    ACCEPT_INVALID_BLOCK__NONCE = "cafebabe" if config_location.__dict__.get('ACCEPT_INVALID_BLOCK', False) else None

    # Share acceptance simulation file
    SIMULATION_FILE = config_location.__dict__.get('SIMULATION_FILE', None)
    SIMULATION_FILTER = config_location.__dict__.get('SIMULATION_FILTER', None)
else:
    ACCEPT_SHARES_ABOVE_TARGET = False
    ACCEPT_INVALID_BLOCK__NONCE = None
    SIMULATION_FILE = None
    SIMULATION_FILTER = None


# ******************** GENERAL SETTINGS ***************

# Destination for application logs, files rotated once per day.
LOGDIR = 'log/'

# Main application log file.
LOGFILE = 'stratum%d.log' % INSTANCE

# Possible values: DEBUG, INFO, WARNING, ERROR, CRITICAL
LOGLEVEL = config_location.LOGLEVEL

# How many threads use for synchronous methods (services).
# 30 is enough for small installation, for real usage
# it should be slightly more, say 100-300.
THREAD_POOL_SIZE = 10

# Port number used for
MANHOLE_TELNET_PORT = 3100 + INSTANCE

ADMIN_PORT = 3200 + INSTANCE

NULL_DATE = datetime.datetime(day=1, month=1, year=1970)

WORKER_PASSWORD_REQUIRED = False

SW_CLIENT_MAX_LENGTH = 32

# connections per second
# expected typical situations: reconnecting 1/4 of pool (~8000) from 4 servers within 20 minutes
DEFAULT_RECONNECT_FREQUENCY = 8000. / 4 / (20 * 60)

# ***************** LOGGING DETAILS *******************

# True/False whether to log each submitted share
LOG_INVALID_SHARE_SUBMIT = False


# ******************** TRANSPORTS *********************

# Port used for Socket transport. Use 'None' for disabling the transport.
LISTEN_SOCKET_TRANSPORT = 3000 + INSTANCE
TCP_PROXY_PROTOCOL = True

# Hostname and credentials for one trusted Bitcoin node ("Satoshi's client").
# Stratum uses both P2P port (which is 8333 already) and RPC port
BITCOIN_TRUSTED_HOST = config_location.BITCOIN_TRUSTED_HOST
BITCOIN_TRUSTED_PORT = config_location.BITCOIN_TRUSTED_PORT
BITCOIN_TRUSTED_USER = config_location.BITCOIN_TRUSTED_USER
BITCOIN_TRUSTED_PASSWORD = config_location.BITCOIN_TRUSTED_PASSWORD
BITCOIN_VERSION_0_9_PLUS = config_location.BITCOIN_VERSION_0_9_PLUS

# ******************** DATABASE *********************

STATS_PUSH_DELAY_GROUP_SIZE = 10
PUSH_SHARES__MINING_SHARES = True
PUSH_SHARES__MINING_SLOTS = False
PUSH_SLOT_SIZE = 300

PRIMARY_DB = {
    'driver': 'MySQLdb',
    'host': config_location.DATABASE_HOST,
    'port': config_location.DATABASE_PORT,
    'dbname': config_location.DATABASE_DBNAME,
    'user': config_location.DATABASE_USER,
    'password': config_location.DATABASE_PASSWORD,
    'pool_size': 5,
}

'''ALTERNATIVE_DB = {
    'driver': 'PostgreSQL',
    'host': config_location.ALT_DATABASE_HOST,
    'port': config_location.ALT_DATABASE_PORT,
    'dbname': config_location.ALT_DATABASE_DBNAME,
    'user': config_location.ALT_DATABASE_USER,
    'password': config_location.ALT_DATABASE_PASSWORD,
    'pool_size': 5,
}'''


# ******************** LOCAL DATABASE *********************

# Full path to database file with local copy of mining worker table
LOCAL_WORKER_DB_FILE = "%s.%d" % (config_location.LOCAL_WORKER_DB_FILE, INSTANCE)

# Number of workers loaded from a database in one group (when
# refreshing the local worker db)
LOCAL_WORKER_DB_GROUP_SIZE = 1000000
# Number of locally refreshed workers before commit
LOCAL_WORKER_DB_REFRESH_COMMIT_SIZE = 1000000
# Time overlap in seconds for selecting workers to be updated by last
# change timestamp (default 30 seconds)
LOCAL_WORKER_DB_REFRESH_TIME_OVERLAP_S = 30

# Pool related settings
INSTANCE_ID = INSTANCE
CENTRAL_WALLET = config_location.CENTRAL_WALLET
PREVHASH_REFRESH_INTERVAL = 1 # in sec
MERKLE_REFRESH_INTERVAL = 30
COINBASE_EXTRAS = ''


BOTNETS = ['bigbang', 'galileo', 'bitharver', 'franknstrein', 'neo03', 'alertopokolo', 'volcvagen', 'cleo', 'corsica', 'hikebit', 'casti', 'terrabits', 'mit9', 'hellout', 'vesuvius', 'myfarm003', 'persio']


# ***************** SHARE MANAGER *********************

SHARE_MANAGER_SHORT_WSMA_PERIOD_S = 10 * 60
SHARE_MANAGER_LONG_WSMA_PERIOD_S = 6 * 60 * 60


# ***************** SHARE LIMITER *********************

LIMITER_MINIMAL_DIFFICULTY = 8
LIMITER_MAXIMAL_DIFFICULTY = 512000
LIMITER_DEFAULT_DIFFICULTY = 32
LIMITER_DEFAULT_DIFFICULTY_STARTUP = 64
LIMITER_STARTUP_PERIOD_S = 120
LIMITER_BLOCK_REQUEST_CANCEL_PERIOD_S = 45
LIMITER_FINE_TUNE_AFTER_S = 600

# Ideal number of submits per second - this is the limiter's target for each connection
LIMITER_TARGET_SUBMISSION_RATE = 16. / 60

# Number of seconds before the limiter tries collects facts about the connections behavior
# to roughly calculate its hash rate.
LIMITER_RECALCULATION_PERIOD_S = 5

# Similar to LIMITER_RECALCULATION_PERIOD but for longer and more accurate calculation.
# This period should be long enough for miners to submit enough times to
# quite accurately calculate their hash rate.
LIMITER_FULL_COLLECTION_PERIOD_S = 120

LIMITER_MIN_SUBMITS_PERCENTILE = 0.05
LIMITER_MIN_OK_SUBMITS_RATIO = 0.7
LIMITER_CHANGE_RATIO_DOWN = 0.8
LIMITER_FINE_TUNE_UPPER_RATIO = 1.33

LIMITER_MAX_SUBMITS_PERCENTILE = 0.95
LIMITER_MAX_OK_SUBMITS_RATIO = 1.65
LIMITER_CHANGE_RATIO_UP = 1.0
LIMITER_FINE_TUNE_LOWER_RATIO = 0.75

# ***************** REPORTING *************************

REPORTER_HOST = config_location.REPORTER_HOST
REPORTER_PORT = config_location.REPORTER_PORT

REPORTER_IGNORED_LOCAL_IPS = config_location.REPORTER_IGNORED_LOCAL_IPS

REPORTER_QUEUE_LENGTH = 16000
REPORTER_ACTIVE_QUEUE_LENGTH = 250

REPORTER_SIMPLE_EVENTS_PERIOD = 2
REPORTER_WORKER_STATS_DELAY = 3
REPORTER_WORKER_STATS_PARTS = 15
REPORTER_SYNC_SESSIONS_BATCH_SIZE = 200
REPORTER_SYNC_SESSIONS_BATCH_DELAY_FUNC = lambda: random.uniform(0.5, 1.5)

REPORTER__WORKER_STATS_CHANGED = False
REPORTER__NEW_AUTHORIZATION = False
REPORTER__NEW_SUBSCRIPTION = False
REPORTER__SESSION_DISCONNECTED = False
REPORTER__DIFFICULTY_CHANGED = False

# ***************** REWARD SYSTEM *********************

SCORE_MAGIC = 1200.  # Belongs to score rewarding system

SHARE_SINK_ENABLED = False
SHARE_SINK_HOST = config_location.SHARE_SINK_HOST
SHARE_SINK_PORT = config_location.SHARE_SINK_PORT

SHARE_TRACE_ENABLED = True
SHARE_TRACE_FILE_NAME_PATTERN = ('%%Y-%%m-%%d/%%H_%s.trace' % STRATUM_UNIQUE_ID)
SHARE_TRACE_FILE_DIR = 'data/'
