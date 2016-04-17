import numpy as np
import multiprocessing

def artest(nlist):
    nlist = np.zeros(1000000)
    nlist = np.arange(1000001, dtype=np.int).reshape(1000001, 1)
    return nlist


lister = []
fh = artest(lister)
print (fh)

#testit = multiprocessing.Process(target=artest, args=lister)
#testit.start()
