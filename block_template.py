import StringIO
import binascii
import struct
import util
import merkletree
import halfnode
from coinbasetx import CoinbaseTransaction

from poolsrv import config

class BlockTemplate(halfnode.CBlock):
    '''Template is used for generating new jobs for clients.
    Let's iterate extranonce1, extranonce2, ntime and nonce
    to find out valid bitcoin block!'''

    coinbase_transaction_class = CoinbaseTransaction

    def __init__(self, timestamper, coinbaser, job_id):
        super(BlockTemplate, self).__init__()

        self.job_id = job_id
        self.timestamper = timestamper
        self.coinbaser = coinbaser

        self.prevhash_bin = '' # reversed binary form of prevhash
        self.prevhash_hex = ''
        self.timedelta = 0
        self.curtime = 0
        self.target = 0
        #self.coinbase_hex = None
        self.merkletree = None

        self.broadcast_args = []

        # Set of 4-tuples (extranonce1, extranonce2, ntime, nonce)
        # registers already submitted and checked shares
        # There may be registered also invalid shares inside!
        self.submits_hash_set = set([])

    def fill_from_another(self, block, prevhash):
        self.height = block.height + 1
        self.nVersion = block.nVersion
        self.hashPrevBlock = int(prevhash, 16)
        self.nBits = block.nBits
        self.hashMerkleRoot = 0
        self.nTime = 0
        self.nNonce = 0

        self.merkletree = merkletree.MerkleTree([None])
        self.coinbase_flags = block.coinbase_flags
        # TODO: replace by some more complete solution
        coinbasevalue = 50*10**8 if self.height < 210000 else 25*10**8
        coinbase = self.coinbase_transaction_class(self.job_id, self.timestamper, self.coinbaser, coinbasevalue,
                        self.coinbase_flags, self.height,
                        config.COINBASE_EXTRAS) #@UndefinedVariable

        self.vtx = [ coinbase, ]
        self.curtime = int(self.timestamper.time()) + block.timedelta
        self.timedelta = block.timedelta
        self.target = block.target

        # Reversed prevhash
        self.prevhash_bin = binascii.unhexlify(util.reverse_hash(prevhash))
        self.prevhash_hex = "%064x" % self.hashPrevBlock

        self.broadcast_args = self.build_broadcast_args()

    def fill_from_rpc(self, data):
        '''Convert getblocktemplate result into BlockTemplate instance'''

        txhashes = [None] + [ util.ser_uint256(int(t['hash'], 16)) for t in data['transactions'] ]
        mt = merkletree.MerkleTree(txhashes)

        self.coinbase_flags = data['coinbaseaux']['flags']
        coinbase = self.coinbase_transaction_class(self.job_id, self.timestamper, self.coinbaser, data['coinbasevalue'],
                        self.coinbase_flags, data['height'],
                        config.COINBASE_EXTRAS) #@UndefinedVariable

        self.height = data['height']
        self.nVersion = data['version']
        self.hashPrevBlock = int(data['previousblockhash'], 16)
        self.nBits = int(data['bits'], 16)
        self.hashMerkleRoot = 0
        self.nTime = 0
        self.nNonce = 0
        self.vtx = [ coinbase, ]

        for tx in data['transactions']:
            t = halfnode.CTransaction()
            t.deserialize(StringIO.StringIO(binascii.unhexlify(tx['data'])))
            self.vtx.append(t)

        self.curtime = data['curtime']
        self.timedelta = self.curtime - int(self.timestamper.time())
        self.merkletree = mt
        self.target = util.uint256_from_compact(self.nBits)

        # Reversed prevhash
        self.prevhash_bin = binascii.unhexlify(util.reverse_hash(data['previousblockhash']))
        self.prevhash_hex = "%064x" % self.hashPrevBlock

        self.broadcast_args = self.build_broadcast_args()

    def fill_from_peer(self, block):
        # Build new block template based on latest block on the same height
        # This is used for generating unique job for miners forcing to the new difficulty
        self.height = block.height
        self.nVersion = block.nVersion
        self.hashPrevBlock = block.hashPrevBlock
        self.nBits = block.nBits
        self.hashMerkleRoot = 0
        self.nTime = 0
        self.nNonce = 0

        self.merkletree = block.merkletree # This is shared with peer template!
        self.coinbase_flags = block.coinbase_flags

        coinbase = self.coinbase_transaction_class(self.job_id, self.timestamper, self.coinbaser, block.get_value(),
                        self.coinbase_flags, self.height,
                        config.COINBASE_EXTRAS) #@UndefinedVariable

        self.vtx = block.vtx[::]
        self.vtx[0] = coinbase

        self.curtime = int(self.timestamper.time()) + block.timedelta
        self.timedelta = block.timedelta
        self.target = block.target

        # Reversed prevhash
        self.prevhash_bin = block.prevhash_bin
        self.prevhash_hex = "%064x" % self.hashPrevBlock

        self.broadcast_args = self.build_broadcast_args()

    # TODO: Proc Template drzi sadu sumbitnutych reseni????
    def register_submit(self, extranonce1, extranonce2, ntime, nonce):
        '''Client submitted some solution. Let's register it to
        prevent double submissions.'''

        t = (extranonce1, extranonce2, ntime, nonce)
        if t not in self.submits_hash_set:
            self.submits_hash_set.add(t)
            return True
        return False

    def build_broadcast_args(self):
        '''Build parameters of mining.notify call. All clients
        may receive the same params, because they include
        their unique extranonce1 into the coinbase, so every
        coinbase_hash (and then merkle_root) will be unique as well.'''
        job_id = self.job_id
        prevhash = binascii.hexlify(self.prevhash_bin)
        (coinb1, coinb2) = [ binascii.hexlify(x) for x in self.vtx[0]._serialized ]
        merkle_branch = [ binascii.hexlify(x) for x in self.merkletree._steps ]
        version = binascii.hexlify(struct.pack(">i", self.nVersion))
        nbits = binascii.hexlify(struct.pack(">I", self.nBits))
        ntime = binascii.hexlify(struct.pack(">I", self.curtime))
        clean_jobs = True

        return (job_id, prevhash, coinb1, coinb2, merkle_branch, version, nbits, ntime, clean_jobs)

    def get_value(self):
        return self.vtx[0].get_value()

    def serialize_coinbase(self, extranonce1, extranonce2):
        '''Serialize coinbase with given extranonce1 and extranonce2
        in binary form'''
        (part1, part2) = self.vtx[0]._serialized
        return part1 + extranonce1 + extranonce2 + part2

    def check_ntime(self, ntime):
        '''Check for ntime restrictions.'''
        if ntime < self.curtime:
            return False

        if ntime > (self.timestamper.time() + 1000):
            # Be strict on ntime into the near future
            # may be unnecessary
            return False

        return True

    def serialize_header(self, merkle_root_int, ntime_bin, nonce_bin):
        '''Serialize header for calculating block hash'''
        r  = struct.pack(">i", self.nVersion)
        r += self.prevhash_bin
        r += util.ser_uint256_be(merkle_root_int)
        r += ntime_bin
        r += struct.pack(">I", self.nBits)
        r += nonce_bin
        return r

    def finalize(self, merkle_root_int, extranonce1_bin, extranonce2_bin, ntime, nonce):
        '''Take all parameters required to compile block candidate.
        self.is_valid() should return True then...'''

        self.hashMerkleRoot = merkle_root_int
        self.nTime = ntime
        self.nNonce = nonce
        self.vtx[0].set_extranonce(extranonce1_bin + extranonce2_bin)
        self.sha256 = None # We changed block parameters, let's reset sha256 cache
