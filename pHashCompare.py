import pandas as pd
import time
import hashlib
import multiprocessing


def ReadCSV(pemail, rhash, output):
    coln = ['Email']
    coln2 = ['Hash']
    pcsv = pd.read_csv(pemail, names=coln)
    rcsv = pd.read_csv(rhash, names=coln2)
    pcsv['Email'] = pcsv['Email'].map(lambda x: x.lower().strip())
    pcsv['Hash'] = pcsv['Email'].map(lambda s: hashlib.sha256(s).hexdigest().upper().strip())
    common = pcsv.merge(rcsv, on=['Hash'])
    results = ['Email']
    common.to_csv(output, sep=',', columns=results, header=False, index=False, encoding='utf-8')


def WriteCSV(filewrite):
    pass


pemailcsv = ('pEmails.csv')
rhashcsv = ('RNhashReturn.csv')
outfile = ('pHashToEmail.csv')
# ------------ MAIN SCRIPT STARTS HERE -----------------
if __name__ == '__main__':
    startTime = time.time()
    results = ReadCSV(pemailcsv, rhashcsv, outfile)
    elapsedTime = time.time() - startTime
    print 'Duration: ', elapsedTime