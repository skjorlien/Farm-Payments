import numpy as np
import matplotlib.pyplot as plt
import matplotlib as mpl
import settings
import os 
from concurrent.futures import ProcessPoolExecutor, as_completed

def testprocess(num): 
    x = num**2
    return f"{x}"

if __name__ == "__main__":
    with ProcessPoolExecutor() as executor:
        results = [executor.submit(testprocess, n) for n in range(1, 1000)]

        for f in as_completed(results):
            print(f.result())
