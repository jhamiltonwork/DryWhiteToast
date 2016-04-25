import numpy as np
import pandas as pd
import hashlib
import time


partner = 'IPSOS'
ckey = 'DgqJYlimINLP'
remail = '/home/snarf/PycharmProjects/DryWhiteToast/DryWhiteToast-RNdev/rsnemail.csv'
phash = '/home/snarf/PycharmProjects/DryWhiteToast/DryWhiteToast-RNdev/pHashed.csv'
output = ('ReturnTo' + partner + '.csv')


def narray():

    narr = np.zeros(500001, dtype=np.int)
    return narr


def carray():

    carr = np.zeros(500001, dtype=str)
    return carr


def combarray():

    c = carray()
    ck = np.core.defchararray.replace(c, '', ckey, count=None)
    cr = ck.reshape(500001, 1)
    n = narray()
    n = np.arange(500001, dtype=np.int)
    ns = n.reshape(500001, 1)
    narr = np.char.mod('%d', ns)
    result2 = np.core.defchararray.add(cr, narr)
    return result2


def cktohash(ckey):

    ckarr = combarray()
    ckdf = pd.DataFrame({'ckey': ckarr[:, 0]})
    ckdf['hash'] = ckdf['ckey']
    ckdf['hash'] = ckdf['hash'].apply(lambda h: hashlib.sha512(h).hexdigest().upper())
    result3 = ckdf.drop('ckey', axis=1)
    return result3


def hemail(remail):

    f = lambda x: x.lower().strip()
    converter = {'email': f}
    ecsv = pd.read_csv(remail, sep=',', header=0, usecols=['email'], dtype=str,
                       skip_blank_lines=True, encoding='utf-8', converters=converter)
    cecsv = ecsv[ecsv['email'].str.contains("@")]
    return cecsv


def hcompare(phash, output):

    hgendf = cktohash(ckey)
    rmatch = pd.read_csv(phash, usecols=['hash'], dtype={'hash': str})
    common = pd.merge(rmatch, hgendf, how='inner', on=['hash'])
    fmatch = (common['hash'])
    hgendf = hgendf[hgendf.hash.isin(fmatch)].index.tolist()
    fiv = str(hgendf).replace('[', '').replace(']', '')
    remail2 = hemail(remail)
    remail2['email'] = ckey + remail2.email.map(str) + fiv
    remail2['hash'] = remail2['email'].apply(lambda h: hashlib.sha512(h).hexdigest().upper())
    remail2.drop('email', axis=1, level=None, inplace=True, errors='raise')
    phashcsv = pd.read_csv(phash, sep=',', header=0, usecols=['hash'], dtype=str,
                           skip_blank_lines=True, encoding='utf-8')
    common2 = phashcsv.merge(remail2, on=['hash'])
    common3 = common2['hash']
    result4 = phashcsv[~phashcsv.hash.isin(common3)]
    toaster = (ckey + fiv)
    toasty = hashlib.sha512(toaster).hexdigest().upper().strip()
    recol = ['hash']
    addkey = pd.DataFrame(columns=recol)
    addkey = addkey.append([{'hash': toasty}])
    cdf = pd.concat([result4, addkey], ignore_index=True)
    cdf.to_csv(output, sep=',', usecols=['hash'], header=True, index=False, dtype=str, skip_blank_lines=True, encoding='utf-8')

#---------------------Main--------------------------
if __name__ == '__main__':
    starttime = time.time()

    hcompare(phash, output)

    endtime = time.time() - starttime
    print('\n')
    print('Duration: ' + str(endtime))