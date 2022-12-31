from setuptools import find_packages, setup

install_requires = [
    #for indicaters
    'numpy',
    'pandas',
    'statsmodels',
    #for rendere
    'matplotlib',
    #for mt5
    'MetaTrader5',
    #for coincheck
    'websocket-client',
    'python-dotenv',
    #for yfinance
    'yfinance'
    #for economic indicators
    'pandas_datareader'
]

setup(name='finance_client',
      version='0.0.1',
      packages=find_packages(),
      data_files=['./finance_client/settings.json', './finance_client/coincheck/.env.template', './finance_client/vantage/resources/physical_currency_list.csv',
                  './finance_client/vantage/resources/digital_currency_list.csv'],
      install_requires=install_requires,
      include_package_data=True
)