import os
import configparser 
from setuptools import setup, find_packages


       
setup(
	name='slurm_utils',
	version='0.1.0',
	maintainer='Mark Lescroart',
	packages=find_packages(),
	description="Slurm calling utilities",
	long_description=open('README.md').read(),
	url='https://github.com/piecesofmindlab/slurm_utils',
	download_url='https://github.com/piecesofmindlab/slurm_utils',
	package_data={
	          'slurm_utils':['defaults.cfg',]
	          },
	)
