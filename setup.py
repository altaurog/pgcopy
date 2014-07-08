from os.path import join, dirname
from setuptools import setup

package_name = "pgcopy"
base_dir = dirname(__file__)

def read(filename):
    f = open(join(base_dir, filename))
    return f.read()

def get_version(package_name, default='0.1'):
    try:
        f = open(join(base_dir, package_name, '__init__.py'))
    except IOError:
        try:
            f = open(join(base_dir, package_name + '.py'))
        except IOError:
            return default
    scope = {}
    exec f in scope
    return scope.get('__version__', default)

setup(
    name = package_name,
    version = get_version(package_name),
    description = "Fast db insert with postgresql binary copy",
    long_description = read("README.rst") + '\n\n' + read("CHANGELOG.txt"),
    author = "Aryeh Leib Taurog",
    author_email = "python@aryehleib.com",
    license = 'MIT',
    url = "http://bitbucket.org/altaurog/pgcopy",
    packages = [package_name],
    install_requires = ["psycopg2", "pytz"],
    classifiers = [
        "Programming Language :: Python",
        "Development Status :: 4 - Beta",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
        "Intended Audience :: Developers",
        "Topic :: Database",
    ],
)
