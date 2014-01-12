
from mdb import Env, KeyNotFoundError, MDB_RDONLY, Cursor, \
        MDB_NEXT, MDB_SET, MDB_FIRST, MDB_SET_RANGE, MDB_GET_BOTH_RANGE, \
        MDB_GET_CURRENT, MDB_PREV
from tempfile import mkdtemp

MAP_SIZE = 1024 * 1024 * 1024 * 20

class Graph(object):
    SPLIT_CHAR = ':'

    def __init__(self, dirname=None):
        if dirname is None:
            dirname = mkdtemp()
        self._env = Env(dirname, mapsize=MAP_SIZE)
        txn = self._env.begin_txn()
        self._nodes = self._env.open_db(txn, 'nodes')
        self._edges = self._env.open_db(txn, 'edges')
        txn.commit()

    def encode_edgepair(self, node1, node2):
        return self.encode_edgeprefix(node1) + node2

    def decode_edgepair(self, edgepair):
        l, k = edgepair.split(self.SPLIT_CHAR, 1)
        l = int(l)
        return (k[:l], k[l:])

    def encode_edgeprefix(self, node1):
        return str(len(node1)) + self.SPLIT_CHAR + node1

    def add_edge(self, node1, node2):
        txn = self._env.begin_txn()
        self._nodes.put(txn, node1, '')
        self._nodes.put(txn, node2, '')
        self._edges.put(txn, self.encode_edgepair(node1, node2), '')
        self._edges.put(txn, self.encode_edgepair(node2, node1), '')
        txn.commit()

    def add_node(self, key):
        txn = self._env.begin_txn()
        self._nodes.put(txn, key, '')
        txn.commit()

    def list_all_edges(self):
        txn = self._env.begin_txn()
        c = Cursor(txn, self._edges)
        op = MDB_FIRST
        while True:
            k, v = c.get(op=op)
            op = MDB_NEXT
            if k is None:
                txn.commit()
                return
            yield k

    def get_node_edges(self, node):
        txn = self._env.begin_txn()
        pfx = self.encode_edgeprefix(node)
        c = Cursor(txn, self._edges)
        started = False
        while True:
            if not started:
                c.get(key=pfx, op=MDB_SET_RANGE)
                k, v = c.get(op=MDB_GET_CURRENT)
                started = True
            else:
                k, v = c.get(op=MDB_NEXT)
            if k is None or not k.startswith(pfx):
                txn.commit()
                return
            yield self.decode_edgepair(k)[1]

