import pandas
import multiprocessing
import time


def ReadMergeCompare(pHash, rHash, output):
    coln = ['Hashes']
    phashcsv = pandas.read_csv('pHashed.csv', names=coln)
    coln2 = ['Hashes']
    rhashcsv = pandas.read_csv('Hashed.csv', names=coln2)
    common = phashcsv.merge(rhashcsv, on=['Hashes'])
    results = phashcsv[(~phashcsv.Hashes.isin(common.Hashes))]


def WriteCSV(filewrite):
    results.to_csv(filewrite, sep='\t', encoding='utf-8')


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
