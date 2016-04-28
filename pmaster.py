import pandas as pd
import random
import hashlib
import multiprocessing
import time
import logging
import sys
import os
import argparse
import datetime
import socket
import re
import uuid
import numpy as np
import string


partner = 'IPSOS'
ckey = 'DgqJYlimINLP'



def ParseCommandLine():
    logging.info('Starting ParseCommandLine')
    parser = argparse.ArgumentParser('...Dry White Toast')

    group = parser.add_mutually_exclusive_group()
    group.add_argument('-v', "--verbose", help="allows progress messages to be displayed", action='store_true')

    # setup arguments
    parser.add_argument('-e', '--emailList', required=False,
                        help="specify the path and name of the csv with the email list for hashing")
    parser.add_argument('-r', '--reportPath', type=ValidateDirectoryWritable, required=True,
                        help="specify the folder/directory where the report will be saved")
    parser.add_argument('-m', '--rsnHashMatch', required=False,
                        help="specify the RSN csv hash list to match to")

    # create a global object to hold the validated arguments, these will be available then

    global gl_args
    global gl_verbose

    gl_args = parser.parse_args()

    if gl_args.verbose:
        gl_verbose = True
    else:
        gl_verbose = False

    if gl_args.emailList:
        if gl_args.rsnHashMatch:
            cktohash()
        else:
            gl_args.rsnHashMatch = False
            combinedf()


def ValidateDirectory(theDir):
    # Validate directory exist
    if not os.path.isdir(theDir):
        raise argparse.ArgumentTypeError('Directory does not exist')
    # Validate the path is readable
    if os.access(theDir, os.R_OK):
        return theDir
    else:
        raise argparse.ArgumentTypeError('Directory is not readable')


def ValidateDirectoryWritable(theDir):
    # Validate the path is a directory
    if not os.path.isdir(theDir):
        raise argparse.ArgumentTypeError('Directory does not exist')
        # Validate the path is writable
    if os.access(theDir, os.W_OK):
        return theDir
    else:
        raise argparse.ArgumentTypeError('Directory is not writable')


def DisplayMessage(msg):
    if gl_verbose:
        print(msg)
    return


def rentry(rlist, rrow):
    rlist = np.zeros(rrow)
    rfill = (rrow + 1)
    rlist = np.arange(rfill, dtype=np.int).reshape(rfill, 1)
    return rlist


def randomentry(size=27, chars=string.ascii_uppercase + string.digits):
    return ''.join(random.choice(chars) for _ in range(size))


def combinedf():
    rdf = randf()
    ehash = buildhash()
    fdf = pd.concat([ehash, rdf], ignore_index=True)
    writeforrsn(fdf)


def randf():
    ranlist = []
    rent = np.random.randint(1, 1000)
    rmask = rentry(ranlist, rent)
    rmask = list(rmask)
    rappendentry = str(randomentry())
    recol = ['email']
    extraentries = pd.DataFrame(columns=recol)
    extraentries['email'] = pd.Series(rmask)
    extraentries['email'] = extraentries.email.map(str)
    extraentries['email'] = extraentries['email'].str.replace('[', rappendentry)
    extraentries['email'] = extraentries['email'].str.replace(']', '@boingboing.com')
    extraentries['hash'] = extraentries['email']
    extraentries['hash'] = extraentries['hash'].apply(lambda h: hashlib.sha512(h).hexdigest().upper())
    extradrop = extraentries.drop('email', axis=1)
    return extradrop


def buildhash():
    cdf = emailwrapper()
    cdf['hash'] = cdf['email']
    cdf['hash'] = cdf['hash'].apply(lambda h: hashlib.sha512(h).hexdigest().upper().strip())
    cdfdrop = cdf.drop('email', axis=1)
    return cdfdrop


def emailwrapper():
    pcsv = readshape()
    initv = np.random.randint(1, 500000)
    nstrng = str(initv)
    pcsv['email'] = ckey + pcsv.email.map(str) + nstrng
    toaster = (ckey + nstrng)
    pcsv = pcsv.append([{'email': toaster}]).reset_index(level=None, drop=True)
    return pcsv


def readshape():
    logging.info('')
    logging.info('Starting readshape function')
    print('Starting to make toast with email' + '\n')
    ecsv = hemail()
    logging.info('Verifying there is only one column')
    DisplayMessage('Verifying there is only one column')
    ecol = str(ecsv.shape[1])
    if (ecsv.shape[1]) == 1:
        logging.info('verified' + '\n')
        DisplayMessage('verified' + '\n')
        return ecsv
    else:
        logging.error('More than one column found ' + 'Number of columns = ' + ecol)
        DisplayMessage('More than one column found ' + 'Number of columns = ' + ecol)
        print('Please remove extra columns from the csv and run again')


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
    pemail = gl_args.emailList
    f = lambda x: x.lower().strip()
    converter = {'email': f}
    ecol = ['email']
    ecsv = pd.read_csv(pemail, sep=',', header=None, names=ecol, dtype=str,
                       skip_blank_lines=True, encoding='utf-8', converters=converter)
    cecsv = ecsv[ecsv['email'].str.contains("@")]
    return cecsv


def fivkey(hgendf):
    rhash = gl_args.rsnHashMatch
    rmatch = pd.read_csv(rhash, usecols=['hash'], dtype={'hash': str})
    common = pd.merge(rmatch, hgendf, how='inner', on=['hash'])
    fmatch = (common['hash'])
    hgendf = hgendf[hgendf.hash.isin(fmatch)].index.tolist()
    fiv = str(hgendf).replace('[', '').replace(']', '')
    remail2 = hemail()
    remail2['email'] = ckey + remail2.email.map(str) + fiv
    remail2['hash'] = remail2['email'].apply(lambda h: hashlib.sha512(h).hexdigest().upper())
    commontores(remail2, fiv)


def partread():
    rhash = gl_args.rsnHashMatch
    pcsv = pd.read_csv(rhash, sep=',', header=0, usecols=['hash'], dtype=str,
                           skip_blank_lines=True, encoding='utf-8')
    return pcsv


def buildtoaster(drytoast):
    toastify = hashlib.sha512(drytoast).hexdigest().upper()
    toastify.strip()
    return toastify


def commontores(phashlist, ivkey):
    rhashcsv = partread()
    common2 = rhashcsv.merge(phashlist, on=['hash'])
    common2.drop('hash', axis=1, level=None, inplace=True, errors='raise')
    common2['email'] = common2['email'].map(lambda x: x.lstrip(ckey).rstrip(ivkey))
    writeresults(common2)


def writeforrsn(pfile):
    outfile = gl_args.reportPath
    output = os.path.join(outfile, "EmailToResearchNow.csv")
    pfile.to_csv(output, sep=',', usecols=['hash'], header=True, index=False, dtype=str,
                 skip_blank_lines=True, encoding='utf-8')


def writeresults(pfile):
    outfile = gl_args.reportPath
    output = os.path.join(outfile, "UsableEmails.csv")
    pfile.to_csv(output, sep=',', usecols=['email'], header=False, index=False, dtype=str,
                 skip_blank_lines=True, encoding='utf-8')


# ------------ MAIN SCRIPT STARTS HERE -----------------
if __name__ == '__main__':
    cdate = datetime.date.today()
    exp = datetime.date(2016, 07, 01)
    daystoexp = (exp - cdate)
    if daystoexp.days > 0:
        startTime = time.time()
        DWT_VERSION = '1.0.0'
        ReleaseDate = "April 11, 2016"

        spath = os.path.realpath(__file__)
        fpath = str(spath)
        lpath = fpath.replace('combined.py', '')
        lpath2 = (lpath + 'Toast.log')

        # Start your logging
        logging.basicConfig(filename=lpath2, level=logging.DEBUG, format='%(asctime)s %(message)s')

        # Record the Start Message
        today = str(cdate)
        expdate = str(daystoexp.days)
        hname = (socket.getfqdn())
        hstrg = str(hname)
        macaddy = (':'.join(re.findall('..', '%012x' % uuid.getnode())))
        mstrg = str(macaddy)
        ipaddy = ([l for l in (
            [ip for ip in socket.gethostbyname_ex(socket.gethostname())[2] if not ip.startswith("127.")][:1], [
                [(s.connect(('8.8.8.8', 53)), s.getsockname()[0], s.close()) for s in
                 [socket.socket(socket.AF_INET, socket.SOCK_DGRAM)]][0][1]]) if l][0][0])
        ipstrg = str(ipaddy)

        logging.info('')
        logging.info('Executing program from: ' + fpath + '\n')
        logging.info('Starting Dry White Toast')
        logging.info('Version: ' + DWT_VERSION)
        logging.info('Release Date: ' + ReleaseDate + '\n')
        logging.info('The license will expire in ' + expdate + ' days' + '\n')
        logging.info('Local host data')
        logging.info('OS flavor:           ' + sys.platform)
        logging.info('Kernel version       ' + sys.version)
        logging.info('')
        logging.info('Computer name:       ' + hstrg)
        logging.info('MAC address:         ' + mstrg)
        logging.info('Current IP address:  ' + ipstrg)
        logging.info('')
        logging.info('')

        print ('Starting Dry White Toast')
        print ('Version: ' + DWT_VERSION)
        print ('Release Date: ' + ReleaseDate + '\n')
        print ('The license will expire in ' + expdate + ' days' + '\n')
        print ('Log file will write to: ' + lpath + '\n')
        print ('Loading toaster...' + '\n')

        parseQuick = multiprocessing.Process(target=ParseCommandLine())
        parseQuick.start()
        parseQuick.join()

        elapsedTime = time.time() - startTime

        # logging.info('Files Processed: ' + str(filesProcessed))
        logging.info('Elapsed Time: ' + str(elapsedTime) + ' seconds')
        logging.info('')
        logging.info('Program Terminated Normally')
        logging.info('')

        # DisplayMessage('Files Processed: ' + str(filesProcessed))
        DisplayMessage('Elapsed Time: ' + str(elapsedTime) + ' seconds')
        DisplayMessage('')
        print ('Program complete')