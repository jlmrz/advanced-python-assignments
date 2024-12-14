from setuptools import setup, find_packages

requirements = [
    'Sphinx~=8.1.3',
    'Pylint~=3.3.1',
    'MyPy~=1.13.0'
]

name = 'stem'
setup(
    name=name,
    version='0.1.0',
    install_requires=requirements,
    packages=find_packages(include=['stem', 'stem.*']),
    command_options={
        'build_sphinx': {
            'project': ('setup.py', name),
            'source_dir': ('setup.py', '.')}},
    entry_points={
        'console_scripts': [
            'stemcli = stem.cli_main:stem_cli_main',
        ],
    }
)