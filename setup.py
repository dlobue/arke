from setuptools import setup, find_packages
import sys, os

version = '0.4.1'

setup(name='arke',
      version=version,
      description="basic server agent for collecting stats",
      #long_description="""TODO""",
      classifiers=[], # Get strings from http://pypi.python.org/pypi?%3Aaction=list_classifiers
      keywords='',
      author='Dominic LoBue',
      author_email='dominic@geodelic.com',
      url='',
      license='proprietary',
      packages=find_packages(exclude=['ez_setup', 'examples', 'tests']),
      include_package_data=True,
      zip_safe=False,
      scripts=['bin/arke'],
      data_files=[('/etc/init', ['init_scripts/arke.conf']),
                 ],
      install_requires=[
          "pymongo>=1.10",
          "gevent>=0.13.6",
          "psutil>=0.3.0",
          "paramiko",
          "boto>=2.0rc1",
          "psycopg2>=2.4.2",
          "simpledaemon",
      ],
      entry_points="""
      # -*- Entry points: -*-
      [arke_plugins]
      latency = arke.plugins.collect.latency
      system = arke.plugins.collect.system
      ssh_hello = arke.plugins.collect.ssh_hello
      mongodb = arke.plugins.collect.mongodb
      postgres_repl = arke.plugins.collect.postgres_repl
      """,
      )
