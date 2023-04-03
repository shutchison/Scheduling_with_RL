from setuptools import setup
from setuptools import find_packages
from setuptools import Extension
from Cython.Build import cythonize

setup(
    name="hpc_env_gym",
    version="0.0.1",
    package_dir={'hpc_env_gym': 'hpc_env_gym'},
    packages=find_packages(),
    
    ext_modules=cythonize([
        Extension('hpc_env_gym.job', ['job.py']),
        Extension('hpc_env_gym.cluster', ['cluster.py']),
        Extension('hpc_env_gym.machine', ['machine.py']),
        Extension('hpc_env_gym.scheduler', ['scheduler.py']),
        Extension('hpc_env_gym.scheduler', ['scheduler.py']),
        Extension('hpc_env_gym.envs.hpc_env', ['hpc_env_gym/envs/hpc_env.py'])
    ], language_level=3),
)