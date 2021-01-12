from os.path import join, dirname
from setuptools import setup

package_name = "pgcopy"
base_dir = dirname(__file__)

def read(filename):
    f = open(join(base_dir, filename))
    return f.read()

def get_version(package_name, default='0.1'):
    try:
        f = open(join(base_dir, package_name, 'version.py'))
    except IOError:
        try:
            f = open(join(base_dir, package_name + '.py'))
        except IOError:
            return default
    for line in f:
        parts = line.split()
        if parts[:2] == ['__version__', '=']:
            return parts[2].strip("'\"")
    return default

setup(
    name = package_name,
    version = get_version(package_name),
    description = "Fast db insert with postgresql binary copy",
    long_description = read("README.rst"),
    long_description_content_type = 'text/x-rst',
    author = "Aryeh Leib Taurog",
    author_email = "python@aryehleib.com",
    license = 'MIT',
    url = "https://pgcopy.readthedocs.io/en/latest/",
    packages = ['pgcopy', 'pgcopy.errors'],
    install_requires = ["psycopg2", "pytz"],
    classifiers = [
        "Programming Language :: Python :: 2",
        "Programming Language :: Python :: 2.7",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Development Status :: 5 - Production/Stable",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
        "Intended Audience :: Developers",
        "Topic :: Database",
    ],
)
