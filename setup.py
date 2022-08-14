from setuptools import setup, find_packages

install_requires = [
    #for indicaters
    'numpy',
    'pandas',
    'stocktrends',
    'statsmodels',
    #for rendere
    'matplotlib',
    #for mt5
    'MetaTrader5',
    #for coincheck
    'websocket-client'
    'rel'
]

setup(name='finance_client',
      version='0.0.1',
      packages=find_packages(),
      data_files=['./finance_client/settings.json', './finance_client/coincheck/.env.template'],
      install_requires=install_requires,
      include_package_data=True
)