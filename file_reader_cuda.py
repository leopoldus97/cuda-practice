import pandas as pd
from numba import cuda
from timeit import default_timer as timer
from forex_python.converter import CurrencyRates

LARGE_FILE_1 = "./data/1000000 Sales Records.csv"
LARGE_FILE_5 = "./data/5m Sales Records.csv"

@cuda.jit(device=True)
def exchangeTo(value, exchange_rate):
    return value * exchange_rate

@cuda.jit
def calculateRecordsParallel(records, exchange_rate):
    start = cuda.grid(1)
    threads = cuda.gridDim.x * cuda.blockDim.x
    for x in range(start, len(records), threads):
        records[x] = exchangeTo(records[x], exchange_rate)

c = CurrencyRates()
exchange_rate = c.get_rate(base_cur='USD', dest_cur='DKK')
records = pd.read_csv(LARGE_FILE_5)
griddim = 128
blockdim = 1024 # Max 1024

print("Start")

start = timer()
d_records = cuda.to_device(records["Total Profit"])
calculateRecordsParallel[griddim,blockdim](d_records, exchange_rate)
prof = d_records.copy_to_host()
tt = timer() - start

print("Time elapsed: %f s" %tt)
print(f"Exchange rate: {exchange_rate}")
print(f"First element before exchange: {records['Total Profit'][0]}")
print(f"First element after exchange: {prof[0]}")
print(f"Result:\n{prof}")
print("End")