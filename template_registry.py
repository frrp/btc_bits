import weakref
import binascii
import util
from decimal import Decimal

from lib.exceptions import SubmitException
from poolsrv.posix_time import posix_time

import poolsrv.logger
log = poolsrv.logger.get_logger()

from mining.interfaces import Interfaces
from extranonce_counter import ExtranonceCounter

from poolsrv import config


class JobIdGenerator(object):
    '''Generate pseudo-unique job_id. Every id must be higher than previous one
    (old_id < new_id), this is used in min_job_id mechanism for forcing difficulty.'''
    counter = 0

    @classmethod
    def get_new_id(cls):
        cls.counter += 1
        #if cls.counter % 0xffffffffff == 0:
        #    # This limit is enough for years of operation
        #    # without an instance restart
        #    cls.counter = 1
        return cls.counter


class TemplateRegistry(object):
    '''Implements the main logic of the pool. Keep track
    on valid block templates, provide internal interface for stratum
    service and implements block validation and submits.'''

    def __init__(self, block_template_class, coinbaser, bitcoin_rpc, instance_id,
                 on_template_callback, on_block_callback):
        self.current_prevhash = ''
        self.jobs = weakref.WeakValueDictionary()
        self.minimal_job_id = 0

        self.extranonce_counter = ExtranonceCounter(instance_id)
        self.extranonce2_size = block_template_class.coinbase_transaction_class.extranonce_size \
                - self.extranonce_counter.get_size()

        self.coinbaser = coinbaser
        self.block_template_class = block_template_class
        self.bitcoin_rpc = bitcoin_rpc
        self.on_block_callback = on_block_callback
        self.on_template_callback = on_template_callback

        self._last_template = None
        self.update_in_progress = False
        self.last_update = None

        # Create first block template on startup
        self.update_block()


    def get_block_min_job_id(self):
        return self.minimal_job_id


    def get_new_extranonce1(self):
        '''Generates unique extranonce1 (e.g. for newly
        subscribed connection.'''
        return self.extranonce_counter.get_new_bin()


    def get_last_template_broadcast_args(self):
        '''Returns arguments for mining.notify
        from last known template.'''
        return self._last_template.broadcast_args


    def get_last_template(self):
        ''' Returns the last known template. '''
        return self._last_template


    def _add_template(self, template):
        '''Adds new template to the registry.
        It also clean up templates which should
        not be used anymore.'''

        prevhash = template.prevhash_hex

        # Did we just detect a new block?
        new_block = (prevhash != self.current_prevhash)
        if new_block:
            # Update the current prevhash and throw away all previous jobs (templates)
            self.current_prevhash = prevhash
            self.jobs = {template.job_id: template}

            # Remember the first job's id for the new block. All next templates must have
            # higher job_id in order to be valid.
            self.minimal_job_id = template.job_id

            # Tell the system about new template
            # It is mostly important for share manager
            self.on_block_callback(prevhash)
        else:
            # Check if the new job (template) has valid job_id (related to the first job for the block)
            if template.job_id < self.minimal_job_id:
                log.error("New template has invalid job_id (%s) - minimal %s" % \
                          (template.job_id, self.minimal_job_id))
                return
            # Remember the job for the current block (prevhash)
            self.jobs[template.job_id] = template

        # Use this template for every new request
        self._last_template = template

        log.info("New template %x (cnt %d) for %s" % \
                 (template.job_id, len(self.jobs), prevhash))

        # Everything is ready, let's broadcast jobs!
        self.on_template_callback(new_block)


    def update_blank_block(self, prevhash):
        '''Pick current block, replaces it's prevhash and broadcast
        it as a new template to client. This is work-around for slow
        processing of blocks in bitcoind.'''

        start = posix_time()

        template = self.block_template_class(Interfaces.timestamper, self.coinbaser, JobIdGenerator.get_new_id())
        template.fill_from_another(self._last_template, prevhash)
        self._add_template(template)

        log.info("Blank block update finished, %.03f sec, %d txes" % \
                    (posix_time() - start, len(template.vtx)))

        # Now let's do standard update (with transactions)
        self.update_block()


    def update_block(self):
        '''Registry calls the getblocktemplate() RPC
        and build new block template.'''

        if self.update_in_progress:
            # Block has been already detected
            return

        self.update_in_progress = True
        self.last_update = posix_time()

        d = self.bitcoin_rpc.getblocktemplate()
        d.addCallback(self._update_block)
        d.addErrback(self._update_block_failed)


    def _update_block_failed(self, failure):
        self.update_in_progress = False
        log.error(str(failure))


    def _update_block(self, bitcoind_rpc_template):
        try:
            if isinstance(bitcoind_rpc_template, dict):
                start = posix_time()

                template = self.block_template_class(Interfaces.timestamper, self.coinbaser, JobIdGenerator.get_new_id())
                template.fill_from_rpc(bitcoind_rpc_template)
                self._add_template(template)

                log.info("Update finished, %.03f sec, %d txes, %s BTC" % \
                         (posix_time() - start, len(template.vtx), Decimal(template.get_value())/10**8)) #@UndefinedVariable
            else:
                log.error("Invalid data for block update: %s" % (bitcoind_rpc_template, ))
        finally:
            self.update_in_progress = False
        return bitcoind_rpc_template



    def diff_to_target(self, difficulty):
        '''Converts difficulty to target'''
        diff1 = 0x00000000ffff0000000000000000000000000000000000000000000000000000
        return diff1 / difficulty


    def get_job(self, job_id):
        '''For given job_id returns BlockTemplate instance or None'''
        try:
            return self.jobs[job_id]
        except:
            log.error("Job id '%s' not found" % job_id)
            return None


    def submit_share(self, job_id, worker_name, extranonce1_bin, extranonce2, ntime, nonce,
                     difficulty):
        '''Check parameters and finalize block template. If it leads
           to valid block candidate, asynchronously submits the block
           back to the bitcoin network.

            - extranonce1_bin is binary. No checks performed, it should be from session data
            - job_id, extranonce2, ntime, nonce - in hex form sent by the client
            - difficulty - decimal number from session, again no checks performed
            - submitblock_callback - reference to method which receive result of submitblock()
        '''

        # Check if extranonce2 looks correctly. extranonce2 is in hex form...
        if len(extranonce2) != self.extranonce2_size * 2:
            raise SubmitException("Incorrect size of extranonce2. Expected %d chars" % (self.extranonce2_size*2))

        # Check for job
        job = self.get_job(job_id)
        if job == None:
            raise SubmitException("Job '%s' not found" % job_id)

        # Check if ntime looks correct
        if len(ntime) != 8:
            raise SubmitException("Incorrect size of ntime. Expected 8 chars")

        if not job.check_ntime(int(ntime, 16)):
            raise SubmitException("Ntime out of range")

        # Check nonce
        if len(nonce) != 8:
            raise SubmitException("Incorrect size of nonce. Expected 8 chars")

        # Check for duplicated submit
        if not job.register_submit(extranonce1_bin, extranonce2, ntime, nonce):
            log.info("Duplicate from %s, (%s %s %s %s)" % \
                    (worker_name, binascii.hexlify(extranonce1_bin), extranonce2, ntime, nonce))
            raise SubmitException("Duplicate share")

        # Now let's do the hard work!
        # ---------------------------
        # 0. Some sugar
        extranonce2_bin = binascii.unhexlify(extranonce2)
        ntime_bin = binascii.unhexlify(ntime)
        nonce_bin = binascii.unhexlify(nonce)

        # 1. Build coinbase
        coinbase_bin = job.serialize_coinbase(extranonce1_bin, extranonce2_bin)
        coinbase_hash = util.doublesha(coinbase_bin)

        # 2. Calculate merkle root
        merkle_root_bin = job.merkletree.withFirst(coinbase_hash)
        merkle_root_int = util.uint256_from_str(merkle_root_bin)

        # 3. Serialize header with given merkle, ntime and nonce
        header_bin = job.serialize_header(merkle_root_int, ntime_bin, nonce_bin)

        # 4. Reverse header and compare it with target of the user
        hash_bin = util.doublesha(''.join([ header_bin[i*4:i*4+4][::-1] for i in range(0, 20) ]))
        hash_int = util.uint256_from_str(hash_bin)
        block_hash_hex = "%064x" % hash_int
        header_hex = binascii.hexlify(header_bin)

        target_user = self.diff_to_target(difficulty)
        if hash_int > target_user:
            # For testing purposes ONLY
            if not config.ACCEPT_SHARES_ABOVE_TARGET:
                raise SubmitException("Share is above target")

        # 5. Compare hash with target of the network
        if hash_int <= job.target or \
                (nonce == config.ACCEPT_INVALID_BLOCK__NONCE):
            # Yay! It is block candidate!
            log.info("We found a block candidate! %s" % block_hash_hex)

            # 6. Finalize and serialize block object
            job.finalize(merkle_root_int, extranonce1_bin, extranonce2_bin, int(ntime, 16), int(nonce, 16))

            if not job.is_valid():
                # Should not happen
                log.error("Final job validation failed!")

            # 7. Get block value for statistical purposes
            block_value = job.get_value()

            # 8. Submit block to the network
            serialized = binascii.hexlify(job.serialize())
            on_submit = self.bitcoin_rpc.submitblock(serialized)

            return (header_hex, block_hash_hex, block_value, on_submit)

        return (header_hex, block_hash_hex, None, None)
