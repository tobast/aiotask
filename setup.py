#!/usr/bin/env python3

from setuptools import setup, find_packages
import sys


def parse_requirements():
    reqs = []
    with open("requirements.txt", "r") as handle:
        for line in handle:
            if line.startswith("-") or line.startswith("git+"):
                continue
            reqs.append(line)
    return reqs


setup(
    name="aiotask",
    version="0.0.2",
    description="A remote, non-distributed, simple asynchronous task queue",
    author="Théophile 'tobast' Bastian",
    license="LICENSE",
    url="https://github.com/tobast/aiotask",
    packages=find_packages(),
    include_package_data=True,
    long_description=open("README.md").read(),
    install_requires=parse_requirements(),
    entry_points={"console_scripts": []},
)
