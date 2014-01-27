
cimport cmdb

cdef cmdb.MDB_env *env

cmdb.mdb_env_create(&env)

print 'io'

