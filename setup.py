#!/usr/bin/env python
"""
Setup script for Apache Beam pipeline
This file is required for Dataflow to install dependencies on worker nodes
"""

from setuptools import setup, find_packages

# Read requirements from file
with open('requirements.txt', 'r') as f:
    requirements = [line.strip() for line in f if line.strip() and not line.startswith('#')]

setup(
    name='mongodb-to-gcs-pipeline',
    version='1.0.0',
    description='Apache Beam pipeline to export MongoDB collections to Google Cloud Storage',
    author='Your Name',
    author_email='your.email@example.com',
    packages=find_packages(),
    install_requires=requirements,
    python_requires='>=3.7',
)
