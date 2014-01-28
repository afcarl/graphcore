
from cpython.mem cimport PyMem_Malloc, PyMem_Realloc, PyMem_Free
from libc.string cimport memcpy, strcmp


cdef class Node:
    cdef bytes _name

    property name:
        def __set__(self, name):
            self._name = name.encode('UTF-8')
            if len(self._name) > 255:
                raise ValueError('Node name may not be longer than 255 characters.')


        def __get__(self):
            return self._name.decode('UTF-8', 'strict')


    def __init__(self, name):
        self.name = name


    def __repr__(self):
        return '(Node: %s)' % self.name


cdef Edge decode_edge(char* encoded_edge, size_t encoded_edge_size):
    cdef int n1_len = <int> encoded_edge[0]

    cdef bytes n1_name = <bytes> encoded_edge[1 : n1_len+1]
    cdef bytes n2_name = <bytes> encoded_edge[n1_len+1 : encoded_edge_size]

    cdef Node n1 = Node.__new__(Node)
    cdef Node n2 = Node.__new__(Node)

    n1._name = n1_name
    n2._name = n2_name

    cdef Edge e = Edge(n1, n2)
    return e


cdef class Edge:
    cdef public Node n1
    cdef public Node n2


    def __init__(self, Node n1, Node n2):
        if strcmp(n1._name, n2._name) > 0:
            self.n2 = n1
            self.n1 = n2
        else:
            self.n1 = n1
            self.n2 = n2


    def __repr__(self):
        return '(Edge: %s %s)' % (self.n1, self.n2)


    cdef char *encode(self):
        cdef bytes n1_name = self.n1._name
        cdef bytes n2_name = self.n2._name

        cdef int mem_needed = 2 + len(n1_name) + len(n2_name)
        cdef char* out_str = <char*> PyMem_Malloc(mem_needed * sizeof(char))

        if not out_str:
            raise MemoryError()

        out_str[0] = <char> len(n1_name)
        out_str[mem_needed - 1] = <char> 0
        
        memcpy(out_str + 1, <char*> n1_name, len(n1_name))
        memcpy(out_str + 1 + len(n1_name), <char*> n2_name, len(n2_name))

        return out_str

