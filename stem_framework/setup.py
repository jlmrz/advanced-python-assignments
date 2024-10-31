from setuptools import setup, find_packages


requirements = [
    'Sphinx~=8.1.3',
    'Pylint~=3.3.1',
    'MyPy~=1.13.0'
]

name = 'stem'
setup(
    name=name,
    install_requires=requirements,
    packages=find_packages(),
   # packages=['core.py', 'meta.py', 'task.py', 'workspace'],
    command_options={
        'build_sphinx': {
            'project': ('setup.py', name),
            'source_dir': ('setup.py', 'doc')}},
)