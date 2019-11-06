from setuptools import setup, find_packages


setup(
        name='eplusparser',
        version='0.1.2',
        description='Parser for EnergyPlus SQL output',
        packages=find_packages(),
        python_requires='>=3.5',
        install_requires=['pandas'],
        extras_require={
            'hdf': ['pytables']
        },
)
