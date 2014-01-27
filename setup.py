
from setuptools import setup
from Cython.Build import cythonize

setup(name='graphcore',
      version='0.0.0',
      packages=['graphcore'],
      ext_modules = cythonize('graphcore/graphcore.pyx')
)

