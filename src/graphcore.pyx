
from cpython.mem cimport PyMem_Malloc, PyMem_Realloc, PyMem_Free
cimport cmdb

include "graph.pyx"

cdef size_t MAP_SIZE = 1024 * 1024 * 1024 * 20
cdef int MAX_DBS = 3


cdef cmdb.MDB_val bytes_to_mdb_val(bytes b):
    cdef cmdb.MDB_val r
    r.mv_size = len(b) + 1
    r.mv_data = <char*> b
    return r


cdef bytes mdb_val_to_bytes(cmdb.MDB_val v):
    cdef bytes b
    b = (<char*> v.mv_data)[:v.mv_size]
    return b


cdef class EdgeSet:
    cdef Graph graph


    def __init__(self, graph):
        self.graph = graph


cdef class NodeIterator:
    cdef Graph graph
    cdef cmdb.MDB_txn *txn
    cdef cmdb.MDB_cursor *cur
    cdef unsigned int op


    def __init__(self, graph):
        self.graph = graph
        self.op = cmdb.MDB_FIRST

        cmdb.mdb_txn_begin(self.graph.env, NULL, cmdb.MDB_RDONLY, &self.txn)
        cmdb.mdb_cursor_open(self.txn, self.graph.nodes_db, &self.cur)
        

    def __next__(self):
        cdef cmdb.MDB_val nodename = bytes_to_mdb_val(<bytes> "")
        cdef cmdb.MDB_val nodeval = bytes_to_mdb_val(<bytes> "")
        cdef int result = cmdb.mdb_cursor_get(self.cur, &nodename, &nodeval, self.op)
        cdef bytes nn
        cdef Node n1

        if result == 0:
            self.op = cmdb.MDB_NEXT
            nn = mdb_val_to_bytes(nodename)
            n1 = Node(nn)
            return n1
        else:
            cmdb.mdb_cursor_close(self.cur)
            cmdb.mdb_txn_commit(self.txn)
            raise StopIteration()


cdef class NodeSet:
    cdef Graph graph


    def __init__(self, graph):
        self.graph = graph


    def add(self, Node n):
        cdef cmdb.MDB_txn *txn
        cdef cmdb.MDB_val nodename = bytes_to_mdb_val(n._name)
        cdef cmdb.MDB_val nodeval = bytes_to_mdb_val(<bytes> "")

        cmdb.mdb_txn_begin(self.graph.env, NULL, 0, &txn)
        cmdb.mdb_put(txn, self.graph.nodes_db, &nodename, &nodeval, 0)
        cmdb.mdb_txn_commit(txn)

    
    def __contains__(self, Node n):
        cdef cmdb.MDB_txn *txn
        cdef cmdb.MDB_val key = bytes_to_mdb_val(n._name)
        cdef cmdb.MDB_val val = bytes_to_mdb_val(<bytes> "")

        cmdb.mdb_txn_begin(self.graph.env, NULL, 0, &txn)
        cdef int r = cmdb.mdb_get(txn, self.graph.nodes_db, &key, &val)
        cmdb.mdb_txn_commit(txn)

        return r == 0


    def __iter__(self):
        return NodeIterator(self.graph)


cdef class Graph:
    cdef cmdb.MDB_env *env
    cdef cmdb.MDB_dbi nodes_db
    cdef cmdb.MDB_dbi edges_db
    cdef cmdb.MDB_dbi meta_db

    cdef NodeSet _nodes
    cdef EdgeSet _edges


    property nodes:
        def __get__(self):
            return self._nodes


    property edges:
        def __get__(self):
            return self._edges


    def __init__(self):
        cdef cmdb.MDB_txn *txn

        self._nodes = NodeSet(self)
        self._edges = EdgeSet(self)

        cmdb.mdb_env_create(&self.env)
        cmdb.mdb_env_set_mapsize(self.env, MAP_SIZE)
        cmdb.mdb_env_set_maxdbs(self.env, MAX_DBS)

        cmdb.mdb_env_open(self.env, "temp", cmdb.MDB_NOSUBDIR, 0664)

        cmdb.mdb_txn_begin(self.env, NULL, 0, &txn)

        cmdb.mdb_dbi_open(txn, "nodes", cmdb.MDB_CREATE, &self.nodes_db)
        cmdb.mdb_dbi_open(txn, "edges", cmdb.MDB_CREATE, &self.edges_db) 
        cmdb.mdb_dbi_open(txn, "meta", cmdb.MDB_CREATE, &self.meta_db)

        cmdb.mdb_txn_commit(txn)






