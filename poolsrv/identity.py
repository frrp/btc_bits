from posix_time import posix_time
import uuid


class Identity(object):

    def __init__(self):
        # Generate a random UUID
        self.uuid = str(uuid.uuid4())
        self.created_at = posix_time()


RUNTIME = Identity()
