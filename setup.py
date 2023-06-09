#!/usr/bin/env python3
from setuptools import setup


def slurp(path):
    with open(path) as f:
        return [line.strip() for line in f]


def requirements():
    return slurp("requirements.txt")


def requirements_dev():
    return slurp("requirements-dev.txt")


setup(
    name="alter_schema",
    version="0.1",
    description="A tool to do online database schema changes",
    url="http://github.com/mdellavo/alter-schema",
    author="Marc DellaVolpe",
    author_email="marc.dellavolpe@gmail.com",
    license="GPLv3",
    install_requires=requirements(),
    tests_require=requirements_dev(),
    packages=["alter_schema"],
    scripts=["bin/alter-schema"],
    zip_safe=True,
)
