import numpy as np
import pandas as pd
import hashlib
import time


partner = 'IPSOS'
ckey = 'DgqJYlimINLP'
remail = 'rsnemail.csv'
phash = 'pHashed.csv'
output = ('ReturnTo' + partner + '.csv')


def narray():
    narr = np.zeros(500001, dtype=np.int)
    narr = np.arange(500001, dtype=np.int)
    nar = np.char.mod('%d', narr)
    return nar


def carray():
    carr = np.zeros(500001, dtype=str)
    ck = np.core.defchararray.replace(carr, '', ckey, count=None)
    return ck


def combarray():
    c = carray()
    n = narray()
    cr = c.reshape(500001, 1)
    ns = n.reshape(500001, 1)
    result2 = np.core.defchararray.add(cr, ns)
    return result2


def cktohash():
    ckarr = combarray()
    ckdf = pd.DataFrame({'ckey': ckarr[:, 0]})
    ckdf['hash'] = ckdf['ckey']
    ckdf['hash'] = ckdf['hash'].apply(lambda h: hashlib.sha512(h).hexdigest().upper())
    result3 = ckdf.drop('ckey', axis=1)
    fivkey(result3)


def hemail():
    f = lambda x: x.lower().strip()
    converter = {'email': f}
    ecsv = pd.read_csv(remail, sep=',', header=0, usecols=['email'], dtype=str,
                       skip_blank_lines=True, encoding='utf-8', converters=converter)
    cecsv = ecsv[ecsv['email'].str.contains("@")]
    return cecsv


def fivkey(hgendf):
    rmatch = pd.read_csv(phash, usecols=['hash'], dtype={'hash': str})
    common = pd.merge(rmatch, hgendf, how='inner', on=['hash'])
    fmatch = (common['hash'])
    hgendf = hgendf[hgendf.hash.isin(fmatch)].index.tolist()
    fiv = str(hgendf).replace('[', '').replace(']', '')
    remail2 = hemail()
    remail2['email'] = ckey + remail2.email.map(str) + fiv
    remail2['hash'] = remail2['email'].apply(lambda h: hashlib.sha512(h).hexdigest().upper())
    remail2.drop('email', axis=1, level=None, inplace=True, errors='raise')
    rmcommon(remail2, fiv)


def partread():
    pcsv = pd.read_csv(phash, sep=',', header=0, usecols=['hash'], dtype=str,
                           skip_blank_lines=True, encoding='utf-8')
    return pcsv


def buildtoaster(drytoast):
    toastify = hashlib.sha512(drytoast).hexdigest().upper()
    toastify.strip()
    return toastify


def rmcommon(rhashlist, ivkey):
    phashcsv = partread()
    common2 = phashcsv.merge(rhashlist, on=['hash'])
    common3 = common2['hash']
    result4 = phashcsv[~phashcsv.hash.isin(common3)]
    toasty = buildtoaster(ivkey)
    recol = ['hash']
    addkey = pd.DataFrame(columns=recol)
    addkey = addkey.append([{'hash': toasty}])
    cdf = pd.concat([result4, addkey], ignore_index=True)
    writeresults(cdf)


def writeresults(returnfile):
    returnfile.to_csv(output, sep=',', usecols=['hash'], header=True, index=False, dtype=str, skip_blank_lines=True, encoding='utf-8')


#---------------------Main--------------------------
if __name__ == '__main__':
    starttime = time.time()

    cktohash()

    endtime = time.time() - starttime
    print('\n')
    print('Duration: ' + str(endtime))
