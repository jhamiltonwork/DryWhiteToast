import pandas as pd
import hashlib
import multiprocessing
import time


def ReadWrite(efile, filewrite):
    coln = ['Email']
    ecsv = pd.read_csv(efile, names=coln)
    ecsv['Email'] = ecsv['Email'].apply(lambda x: x.lower().strip())
    ecsv['Hash'] = ecsv['Email'].apply(lambda s: hashlib.sha256(s).hexdigest().upper().strip())
    results = ['Hash']
    ecsv.to_csv(filewrite, sep=',', columns=results, header=False, index=False, encoding='utf-8')


def ReadMergeCompare(pHash, rHash, output):
    coln = ['Hash']
    phashcsv = pandas.read_csv(pHash, names=coln)
    coln2 = ['Hash']
    rhashcsv = pandas.read_csv(rHash, names=coln2)
    common = phashcsv.merge(rhashcsv, on=['Hash'])
    results = phashcsv[(~phashcsv.Hash.isin(common.Hash))]
    results.to_csv(output, sep=',', header=False, index=False, encoding='utf-8')


emailList = ('EmailTest.csv')
output = ('Hashed.csv')

# ------------ MAIN SCRIPT STARTS HERE -----------------
if __name__ == '__main__':
    startTime = time.time()
    emailsProcessed = multiprocessing.Process(target=ReadWrite(emailList, output))
    emailsProcessed.start()
    elapsedTime = time.time() - startTime

    print ('Duration: ', elapsedTime)
