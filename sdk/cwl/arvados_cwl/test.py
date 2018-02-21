import time

def operation1(x):
    time.sleep(1)
    print("1" + x)

def operation2():
    time.sleep(2)
    raise Exception("hi")
    print("2")

import threading
import concurrent.futures

from concurrent.futures import ThreadPoolExecutor, wait

with ThreadPoolExecutor() as tp:
    op1 = tp.submit(operation1, "hi")
    op2 = tp.submit(operation2)

    wait([op1, op2])

    op2.result()
