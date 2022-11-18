# CSV Client

CSV Client to handle Time Series data of OHLC.
```
client = CSVClient(files=csv_file)
client.get_ohlc()
```

||Close|High|Low|Open
|--|--|--|--|--|
||1812.T|1963.T|1812.T|1963.T|1812.T|1963.T|1812.T|1963.T
|Date||||||||||||
|2000-01-04|596.0|246.0|620.0|252.0|594.0|246.0|600.0|248.0|
|2000-01-05|610.0|262.0|610.0|263.0|600.0|242.0|600.0|245.0|
|2000-01-06|600.0|254.0|614.0|267.0|596.0|250.0|612.0|267.0|
|...|||||||||

## Loading Data

You can specify file path(s) when initialize a Client or get ohlc data.

```
client = CSVClient(files=csv_file[0])
client.get_rates(symbols=csv_files[1])
```