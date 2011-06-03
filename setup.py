from setuptools import setup, find_packages
import sys, os

version = '0.1'

setup(name='arke',
      version=version,
      description="basic server agent for collecting stats",
      long_description="""\
TODO""",
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
      install_requires=[
          "timer2",
          "yapsy",
          "simpledaemon",
          "pymongo",
      ],
      entry_points="""
      # -*- Entry points: -*-
      """,
      )
