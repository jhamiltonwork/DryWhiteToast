import pandas
import multiprocessing
import time


def ReadMergeCompare(pHash, rHash, output):
    coln = ['Hash']
    phashcsv = pandas.read_csv(pHash, names=coln)
    coln2 = ['Hash']
    rhashcsv = pandas.read_csv(rHash, names=coln2)
    common = phashcsv.merge(rhashcsv, on=['Hash'])
    results = phashcsv[(~phashcsv.Hash.isin(common.Hash))]
    results.to_csv(output, sep=',', header=False, index=False, encoding='utf-8')


def WriteCSV(filewrite):
    pass
    filewrite.to_csv(filewrite, sep=False, encoding='utf-8')


pCSV = ('pHashed.csv')
rCSV = ('Hashed.csv')
oCSV = ('RNhashReturn.csv')
# ------------ MAIN SCRIPT STARTS HERE -----------------
if __name__ == '__main__':
    startTime = time.time()
    hashcomparesults = multiprocessing.Process(target=ReadMergeCompare(pCSV, rCSV, oCSV))
    hashcomparesults.start()
    elapsedTime = time.time() - startTime
    print 'Duration: ', elapsedTime
