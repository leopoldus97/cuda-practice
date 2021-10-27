import pandas as pd
import multiprocessing as mp
from forex_python.converter import CurrencyRates
from timeit import default_timer as timer

LARGE_FILE_1 = "./data/1000000 Sales Records.csv"
LARGE_FILE_5 = "./data/5m Sales Records.csv"
CHUNKSIZE = 100000

# start = timer()
# with open(LARGE_FILE_5) as csv_file:
#     csv_reader = csv.reader(csv_file, delimiter=',')
#     line_count = 0
#     for row in csv_reader:
#         if row:
#             line_count += 1
#     print(f"Processed {line_count} lines.")
# tt = timer() - start
# print("Time elapsed: %f s" % tt)

# ---------------

# start = timer()
# df = pd.read_csv(LARGE_FILE_5, chunksize=CHUNKSIZE)
# total_length = 0
# for chunk in df:
#     total_length += len(chunk)
# tt = timer() - start
# print("Time elapsed: %f s" % tt)
# print(total_length)

# ---------------

def exchangeTo(value, exchange_rate):
    return value * exchange_rate

def calculateRecordsParallel(records, exchange_rate):
    for x in range(0, len(records)):
        records[x] = exchangeTo(records[x], exchange_rate)
    return records

if __name__ == '__main__':
    c = CurrencyRates()
    exchange_rate = c.get_rate('USD', 'DKK')
    records = pd.read_csv(LARGE_FILE_5)["Total Profit"]

    print("Start")

    start = timer()
    pool = mp.Pool(mp.cpu_count())
    result = pool.apply_async(calculateRecordsParallel, (records, exchange_rate))
    pool.close()
    pool.join()
    tt = timer() - start
    
    print("Time elapsed: %f s" % tt)
    print(f"Exchange rate: {exchange_rate}")
    print(f"First element before exchange: {records[0]}")
    print(f"First element after exchange: {result.get()[0]}")
    print(f"Result:\n{result.get()}")
    print("End")

# def func():
#     reader = pd.read_table(LARGE_FILE_5, chunksize=CHUNKSIZE)
#     pool = mp.Pool(mp.cpu_count())

#     funclist = []
#     for df in reader:
#         f = pool.apply_async(process_frame, [df])
#         funclist.append(f)

#     result = 0
#     for f in funclist:
#         result += f.get(timeout=10)
#     return result