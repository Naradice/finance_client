from setuptools import find_packages, setup

install_requires = [
    # for indicaters
    "numpy",
    "pandas",
    "statsmodels",
    # for economic indicators
    "pandas_datareader",
]

setup(
    name="fprocess",
    version="0.0.1",
    packages=find_packages(),
    data_files=[],
    install_requires=install_requires,
    include_package_data=True,
)
