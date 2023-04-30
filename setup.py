import os.path
import re
from pathlib import Path

from setuptools import find_packages, setup

this_directory = Path(__file__).parent
long_description = (this_directory / 'README.md').read_text()


def read(*parts):
    with open(os.path.join(*parts)) as f:
        return f.read().strip()


def read_version():
    regexp = re.compile(r"^__version__\W*=\W*\'([\d.abrc]+)\'")
    init_py = os.path.join(os.path.dirname(__file__), 'aiodistributor', '__init__.py')
    with open(init_py) as f:
        for line in f:
            match = regexp.match(line)
            if match is not None:
                return match.group(1)
        raise RuntimeError(f'Cannot find version in {init_py}')


classifiers = [
    'License :: OSI Approved :: Apache Software License',
    'Development Status :: 2 - Pre-Alpha',
    'Programming Language :: Python',
    'Programming Language :: Python :: 3.10',
    'Programming Language :: Python :: 3.11',
    'Programming Language :: Python :: 3 :: Only',
    'Operating System :: POSIX',
    'Environment :: Web Environment',
    'Intended Audience :: Developers',
    'Topic :: Software Development',
    'Topic :: Software Development :: Libraries',
    'Framework :: AsyncIO',
]

setup(
    name='aiodistributor',
    version=read_version(),
    description='Python asynchronous library for synchronizing replicated microservices',
    long_description=long_description,
    long_description_content_type='text/markdown',
    classifiers=classifiers,
    platforms=['POSIX'],
    url='https://github.com/malik89303/aiodistributor',
    license='Apache License 2.0',
    packages=find_packages(exclude=['tests', 'examples']),
    install_requires=[
        'redis',
        'ujson',
    ],
    package_data={'aiodistributor': ['py.typed']},
    python_requires='>=3.10',
    include_package_data=True,
)
