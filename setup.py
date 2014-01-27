
from setuptools import setup, Extension

from Cython.Distutils import build_ext

graphcore = Extension(
    'graphcore',
    ['src/graphcore.pyx','lib/mdb/libraries/liblmdb/mdb.c'],
    libraries = ['lmdb'],
    library_dirs = ['lib/mdb/libraries/liblmdb'],
    include_dirs = ['lib/mdb/libraries/liblmdb'],
)

setup(name='graphcore',
      version='0.0.0',
      cmdclass = {'build_ext': build_ext},
      ext_modules = [graphcore],
)

