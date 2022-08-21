import re
from setuptools import setup

with open('README.md') as f:
    readme = f.read()

version = ''
with open('asqlite/__init__.py') as f:
    version = re.search(r'^__version__\s*=\s*[\'"]([^\'"]*)[\'"]', f.read(), re.MULTILINE).group(1)

setup(
    name='asqlite',
    author='Rapptz',
    url='https://github.com/Rapptz/asqlite/',
    project_urls={
        'Issue Tracker': 'https://github.com/Rapptz/asqlite/issues/',
    },
    version=version,
    packages=['asqlite'],
    package_data={'asqlite': ['py.typed']},
    license='MIT',
    description='A simple and easy to use async wrapper for sqlite3.',
    long_description=readme,
    long_description_content_type='text/markdown',
    include_package_data=True,
    python_requires='>=3.8.0',
    classifiers=[
        'License :: OSI Approved :: MIT License',
        'Intended Audience :: Developers',
        'Natural Language :: English',
        'Operating System :: OS Independent',
        'Programming Language :: Python :: 3.8',
        'Programming Language :: Python :: 3.9',
        'Programming Language :: Python :: 3.10',
    ]
)
