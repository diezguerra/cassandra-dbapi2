# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


from Queue import Queue, Empty
from contextlib import contextmanager
from multiprocessing import Lock
from random import choice
from sys import platform
from threading import Thread
from time import sleep

import cql


__all__ = ['ConnectionPool', 'ConnectionPoolSingleton']


class ConnectionPool(object):
    """
    Simple connection-caching pool implementation.

    ConnectionPool provides the simplest possible connection pooling,
    lazily creating new connections if needed as `borrow_connection' is
    called.  Connections are re-added to the pool by `return_connection',
    unless doing so would exceed the maximum pool size.

    Example usage:
    [1]: pool = ConnectionPool("localhost", 9160, "Keyspace1")
    [2]: conn = pool.borrow_connection()
    [3]: conn.execute(...)
    [4]: pool.return_connection(conn)
    """

    lock = Lock()

    def __init__(self, hostname='127.0.0.1', port=9160, keyspace=None,
                 user=None, password=None, cql_version='3.0.0',
                 compression=None, consistency_level='ONE', transport=None,
                 max_conns=25, max_idle=5, eviction_delay=10000):
        self.hostname = hostname \
            if isinstance(hostname, (tuple, list)) else [hostname]
        self.port = port
        self.keyspace = keyspace
        self.user = user
        self.password = password
        self.cql_version = cql_version
        self.compression = compression
        self.consistency_level = consistency_level
        self.transport = transport
        self.max_conns = max_conns
        self.max_idle = max_idle
        self.eviction_delay = eviction_delay

        self.connections = Queue(maxsize=0)  # Infinite, for now
        self.eviction = Eviction(self.connections,
                                 self.max_idle,
                                 self.eviction_delay)

    def __create_connection(self):
        return cql.connect(
            choice(self.hostname),
            port=self.port,
            keyspace=self.keyspace,
            user=self.user,
            password=self.password,
            cql_version=self.cql_version,
            compression=self.compression,
            consistency_level=self.consistency_level,
            transport=self.transport)

    def borrow_connection(self):
        try:
            with self.lock:
                connection = self.connections.get(block=False)
        except Empty:
            connection = self.__create_connection()
        return connection

    def return_connection(self, connection):
        with self.lock:
            self.connections.put(connection)
        return
        # No .qsize() in OSX, so we always close
        if platform == 'darwin' or \
                self.connections.qsize() > self.max_conns:
            connection.close()
            return
        if not connection.open_socket:
            return
        with self.lock:
            self.connections.put(connection)

    @property
    @contextmanager
    def connection(self):
        """This method creates a instance property that is in turn a context
        generator that borrows and finally returns a connection back to the
        pool.

        Usage:

        [1]: with pool.connection as conn:
        [2]:    print conn.cursor.execute('USE aps;')

        True

        """

        conn = self.borrow_connection()
        yield conn
        self.return_connection(conn)


class ConnectionPoolSingleton(ConnectionPool):
    _instance = None

    def __new__(cls, *args, **kwargs):
        if not cls._instance:
            cls._instance = super(ConnectionPoolSingleton, cls).__new__(
                cls, *args, **kwargs)
        return cls._instance


class Eviction(Thread):
    def __init__(self, connections, max_idle, eviction_delay):
        Thread.__init__(self)

        self.connections = connections
        self.max_idle = max_idle
        self.eviction_delay = eviction_delay

        self.setDaemon(True)
        self.setName("EVICTION-THREAD")

        # No .qsize() in OSX, so no point checking `max_idle`
        if platform != 'darwin':
            self.start()

    def run(self):
        while True:
            while(self.connections.qsize() > self.max_idle):
                with self.lock:
                    connection = self.connections.get(block=False)
                if connection:
                    if connection.open_socket:
                        connection.close()
            sleep(self.eviction_delay / 1000)
