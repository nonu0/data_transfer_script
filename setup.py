from setuptools import setup, find_packages

setup(
    name='data_transfer_script',
    version='1.0.0',
    author='Cliff Ogola',
    author_email='cliffowino22@gmail.com',
    description='Script to fetch data from MSSQL database and transfer it to Import Users API',
    packages=find_packages(),
    install_requires=[
        'pyodbc',
        'requests',
        'datetime',
        'json',
    ],
    entry_points={
        'console_scripts': [
            'data_transfer_script = data_transfer_script:main',
        ],
    },
)
