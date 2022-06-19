from setuptools import setup, find_packages

install_requires = [
    'numpy',
    'pandas',
    'stocktrends',
    'statsmodels'
]

setup(name='finance_client',
      version='0.0.1',
      packages=find_packages(),
      data_files=['./finance_client/settings.json', './finance_client/coincheck/.env.template'],
      install_requires=install_requires,
      include_package_data=True
)