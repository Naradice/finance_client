# finance process

Modules to handle 3 types of processes for data handling of finance data (i.e. OHLC) with pandas

## 1. Technical Indicator Process

Caliculate Technical Indicater from OHLC DataFrame

1. MACD
2. EMA
3. Bollinger BAND
4. ATR
5. RSI
6. Renko
7. Slope

```python

df = pd.read_csv(file)

macd = MACDProcess()
bb = BBandProcess()
processes = [macd, bb]

for process in processes:
    df = process.run(df)
```

## 2. Transform finance data

For standalization, roll time span etc, sometimes we need to transform finance data. You can conbine #1 and #2 processes.

```python

df = pd.read_csv(file)
ohlc_columns = ["Open", "High", "Low" "Close"]

roll = RollPreProcess(freq="30MIN")
macd = MACDProcess()
bb = BBandProcess()
standalization = MiniMaxPreProcess(columns=[*ohlc_columns, *bb.columns])

processes = [roll, macd, bb, standalization]

for process in processes:
    df = process.run(df)
```

## 3. Add Economic Indicator Process (Under Implementation)

Download indicater values and store it as a CSV to utilize it later without network access. Then return Economic indicater values for same time range.

## 4 Planned

Check Process: check if OHLC data index is which timezone, how many missing data etc.
