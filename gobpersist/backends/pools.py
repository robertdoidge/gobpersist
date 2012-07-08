# pools.py - Generic connection pooling for use by back ends
# Copyright (C) 2012 Accellion, Inc.
#
# This library is free software; you can redistribute it and/or modify
# it under the terms of the GNU Lesser General Public License as
# published by the Free Software Foundation; version 2.1.
#
# This library is distributed in the hope that it will be useful, but
# WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
# Lesser General Public License for more details.
#
# You should have received a copy of the GNU Lesser General Public
# License along with this library; if not, write to the Free Software
# Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA
# 02110-1301 USA
"""Connection-sharing pools for use by session back ends.

.. moduleauthor:: Evan Buswell <evan.buswell@accellion.com>
"""

import thread
import threading
import contextlib
import time

class SimpleThreadMappedPool(object):
    def __init__(self, client, keeptime=180):
        """
        Args:
          ``client``: The class of the client object.

             We will be instantiating these based on the ``args`` and
             ``kwargs`` passed in to :func:`reserve`.
        """
        self.pool = {}
        self.client = client
        self.keeptime = keeptime

    def prune(self):
        live_thread_ids = set([thread.ident for thread in threading.enumerate() \
                                   if thread.ident is not None])
        for thread_id in [tid for tid in self.pool.iterkeys() \
                              if tid not in live_thread_ids]:
            del self.pool[thread_id]

    def alloc_client(self, client_hash, args, kwargs):
        self.prune()
        client_hash['args'] = args
        client_hash['kwargs'] = kwargs
        client_hash['client'] = self.client(*args, **kwargs)
        client_hash['time'] = time.time()

    @contextlib.contextmanager
    def reserve(self, *args, **kwargs):
        """Reserve a client.

        The arguments passed in will be used for class initialization
        if necessary.  Note that this function is a
        ``contextmanager``, hence should be called as::

           with pool.reserve("some", "args") as client:
              # do client stuff
           # connection has been returned to the pool.
        """
        thread_id = thread.get_ident()
        if thread_id not in self.pool:
            # create a new Client
            client_hash = {}
            self.alloc_client(client_hash, args, kwargs)
            self.pool[thread_id] = client_hash
            yield client_hash['client']
        else:
            client_hash = self.pool[thread_id]
            if client_hash['args'] != args or client_hash['kwargs'] != kwargs \
                    or client_hash['client'] is None \
                    or client_hash['time'] + self.keeptime < time.time():
                # Close the connection
                self.client_close(client_hash['client'])
                client_hash['client'] = None
                self.alloc_client(client_hash, args, kwargs)
            yield client_hash['client']

    def relinquish(self):
        """Relinquish any claim to a client.

        In this implementation, it will simply close the current
        thread's client.
        """
        thread_id = thread.get_ident()
        if thread_id in self.pool:
            client_hash = self.pool[thread_id]
            self.client_close(client_hash['client'])
            client_hash['client'] = None
            del self.pool[thread_id]

    def client_close(self, client):
        # Try to close the existing connection using the
        # standard methods.
        if hasattr(client, 'close'):
            client.close()
        else: # Or let gc clean up the resources.
            del client
            client = None

        return client
