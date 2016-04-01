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


emailList = ('pEmails.csv')
output = ('pHashed.csv')


#------------ MAIN SCRIPT STARTS HERE -----------------
if __name__ == '__main__':
    startTime = time.time()
    emailsProcessed = multiprocessing.Process(target=ReadWrite(emailList, output))
    emailsProcessed.start()
    elapsedTime = time.time() - startTime
    print 'Duration: ', elapsedTime