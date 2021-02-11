from setuptools import setup, find_packages


with open('README.md') as f:
    readme = f.read()

with open('LICENSE') as f:
    license_file = f.read()

setup(
    name='py-event-bus-postgres',
    version='0.1.0',
    description='Postgres Event Bus for Python',
    python_requires=">=3.6",
    long_description=readme,
    author='Drew Lindsey',
    author_email='drew.a.lindsey@gmail.com',
    url='https://github.com/DrewLindsey/py-event-bus-postgres',
    license=license_file,
    packages=find_packages(exclude=('tests', 'docs'))
)