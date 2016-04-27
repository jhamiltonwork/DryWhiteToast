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
            pHashToEmail(gl_args.emailList, gl_args.rsnHashMatch, gl_args.reportPath)
        else:
            gl_args.rsnHashMatch = False
            ReadWrite(gl_args.emailList, gl_args.reportPath)


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


def ReadWrite(elist, outfile):
    try:
        logging.info('')
        logging.info('Starting ReadWrite Function')
        print('Starting to make toast with email' + '\n')
        logging.info('Creating output file path')
        DisplayMessage('Creating output file path')
        output = os.path.join(outfile, "pHashed.csv")
        logging.info('Complete' + '\n')
        DisplayMessage('Complete' + '\n')
        logging.info('Reading csv file into DataFrame')
        DisplayMessage('Reading csv file into DataFrame')
        coln = ['Email']
        ecsv = pd.read_csv(elist, names=coln)
        logging.info('Complete' + '\n')
        DisplayMessage('Complete' + '\n')
        logging.info('Storing the shape of the Dataframe')
        DisplayMessage('Storing the shape of the Dataframe')
        erows = str(ecsv.shape[0])
        ecol = str(ecsv.shape[1])
        logging.info('Complete' + '\n')
        DisplayMessage('Complete' + '\n')
        logging.info('Verifying there is only one column')
        DisplayMessage('Verifying there is only one column')
        if (ecsv.shape[1]) == 1:
            logging.info('verified' + '\n')
            DisplayMessage('verified' + '\n')
            logging.info('Verifying that all csv rows contain emails')
            DisplayMessage('Verifying that all csv rows contain emails')
            cecsv = ecsv[ecsv['Email'].str.contains("@")]
            crows = str(cecsv.shape[0])
            if erows > crows:
                logging.warning(crows + ' emails found in ' + erows + ' rows' + '\n')
                DisplayMessage(crows + ' emails found in ' + erows + ' rows' + '\n')
                logging.info('Selecting only rows with emails')
                DisplayMessage('Selecting only rows with emails')
                common = cecsv.merge(ecsv, on=['Email'])
                logging.info('complete' + '\n')
                DisplayMessage('complete' + '\n')
                logging.info('Generating Initializing Vector and FingerPrint')
                DisplayMessage('Generating Initializing Vector and FingerPrint')
                rnfing = ('DgqJYlimINLP')
                initv = np.random.randint(1,500000)
                nstrng = str(initv)
                logging.info('complete' +'\n')
                DisplayMessage('complete' + '\n')
                logging.info('Fingerprint: ' + rnfing)
                DisplayMessage('Fingerprint: ' + rnfing)
                logging.info('IV: ' + nstrng + '\n')
                DisplayMessage('IV: ' + nstrng + '\n')
                logging.info('Removing all white spaces and making all emails lower cased')
                DisplayMessage('Removing all white spaces and making all emails lower cased')
                common['Email'] = common['Email'].apply(lambda x: x.lower().strip())
                logging.info('complete' +'\n')
                DisplayMessage('complete' + '\n')
                logging.info('Appending fingerprint to front of emails')
                DisplayMessage('Appending fingerprint to front of emails')
                common['Email'] = rnfing + common.Email.map(str)
                logging.info('complete' +'\n')
                DisplayMessage('complete' + '\n')
                logging.info('Appending Initializing Vector to end of emails')
                DisplayMessage('Appending Initializing Vector to end of emails')
                common['Email'] = common.Email.map(str) + nstrng
                logging.info('complete' + '\n')
                DisplayMessage('complete' + '\n')
                logging.info('Loading toaster')
                DisplayMessage('Loading toaster')
                toaster = (rnfing + nstrng)
                common = common.append([{'Email': toaster}])
                logging.info('complete' + '\n')
                DisplayMessage('complete' + '\n')
                logging.info('Creating sha512 hash id for each email string')
                DisplayMessage('Creating sha512 hash id for each email string')
                common['Hash'] = common['Email'].apply(lambda h: hashlib.sha512(h).hexdigest().upper().strip())
                logging.info('complete' + '\n')
                DisplayMessage('complete' + '\n')
                logging.info('Generate random entries')
                DisplayMessage('Generate random entries')
                ranlist = []
                rent = np.random.randint(1, 1000)
                rmask = rentry(ranlist, rent)
                rmask = list(rmask)
                logging.info('complete' + '\n')
                DisplayMessage('complete' + '\n')
                logging.info('Adding entries')
                DisplayMessage('Adding entries')
                recol = ['Email']
                extraentries = pd.DataFrame(columns=recol)
                extraentries['Email'] = pd.Series(rmask)
                logging.info('complete' + '\n')
                DisplayMessage('complete' + '\n')
                logging.info('Appending random alphanumeric to random entry')
                DisplayMessage('Appending random alphanumeric to random entry')
                rappendentry = str(randomentry())
                extraentries['Email'] = extraentries.Email.map(str)
                extraentries['Email'] = extraentries['Email'].str.replace('[', rappendentry)
                logging.info('complete' + '\n')
                DisplayMessage('complete' + '\n')
                logging.info('Appending email tag')
                DisplayMessage('Appending email tag')
                extraentries['Email'] = extraentries['Email'].str.replace(']', '@boingboing.com')
                logging.info('complete' + '\n')
                DisplayMessage('complete' + '\n')
                logging.info('Creating Hash Column')
                DisplayMessage('Creating Hash Column')
                extraentries['Hash'] = extraentries['Email'].apply(lambda h: hashlib.sha512(h).hexdigest().upper().strip())
                logging.info('complete' + '\n')
                DisplayMessage('complete' + '\n')
                logging.info('Concatenating two dataframes')
                DisplayMessage('Concatenating two dataframes')
                cdf = pd.concat([common, extraentries], ignore_index=True)
                logging.info('complete' + '\n')
                DisplayMessage('complete' + '\n')
                logging.info('Sorting by Hash')
                DisplayMessage('Sorting by Hash')
                adf = cdf.sort(['Hash'], ascending=True)
                logging.info('complete' + '\n')
                DisplayMessage('complete' + '\n')
                logging.info('Writing hash list to csv in ' + output)
                DisplayMessage('Writing hash list to csv in ' + output)
                results = ['Hash']
                adf.to_csv(output, sep=',', columns=results, header=False, index=False, encoding='utf-8')
                logging.info('complete' + '\n')
                DisplayMessage('complete' + '\n')
            else:
                logging.info('Verified' + '\n')
                DisplayMessage('Verified' + '\n')
                rnfing = str('DgqJYlimINLP')
                initv = initv = np.random.randint(1,500000)
                nstrng = str(initv)
                logging.info('complete' +'\n')
                DisplayMessage('complete' + '\n')
                logging.info('Fingerprint: ' + rnfing)
                DisplayMessage('Fingerprint: ' + rnfing)
                logging.info('IV: ' + nstrng + '\n')
                DisplayMessage('IV: ' + nstrng + '\n')
                logging.info('Removing all white spaces and making all emails lower cased')
                DisplayMessage('Removing all white spaces and making all emails lower cased')
                ecsv['Email'] = ecsv['Email'].apply(lambda x: x.lower().strip())
                logging.info('complete' + '\n')
                DisplayMessage('complete' + '\n')
                logging.info('Appending fingerprint to front of emails')
                DisplayMessage('Appending fingerprint to front of emails')
                ecsv['Email'] = rnfing + ecsv.Email.map(str)
                logging.info('complete' +'\n')
                DisplayMessage('complete' + '\n')
                logging.info('Appending Initializing Vector to end of emails')
                DisplayMessage('Appending Initializing Vector to end of emails')
                ecsv['Email'] = ecsv.Email.map(str) + nstrng
                logging.info('complete' + '\n')
                DisplayMessage('complete' + '\n')
                logging.info('Adding toaster')
                DisplayMessage('Adding toaster')
                toaster = (rnfing + nstrng)
                ecsv = ecsv.append([{'Email':toaster}])
                logging.info('complete' + '\n')
                DisplayMessage('complete' + '\n')
                logging.info('Creating sha512 hash id for each email string')
                DisplayMessage('Creating sha512 hash id for each email string')
                ecsv['Hash'] = ecsv['Email'].apply(lambda s: hashlib.sha512(s).hexdigest().upper().strip())
                logging.info('complete' + '\n')
                DisplayMessage('complete' + '\n')
                logging.info('Generate random entries')
                DisplayMessage('Generate random entries')
                ranlist = []
                rent = np.random.randint(1, 1000)
                rmask = rentry(ranlist, rent)
                rmask = list(rmask)
                logging.info('complete' + '\n')
                DisplayMessage('complete' + '\n')
                logging.info('Adding entries')
                DisplayMessage('Adding entries')
                recol = ['Email']
                extraentries = pd.DataFrame(columns=recol)
                extraentries['Email'] = pd.Series(rmask)
                logging.info('complete' + '\n')
                DisplayMessage('complete' + '\n')
                logging.info('Appending random alphanumeric to random entry')
                DisplayMessage('Appending random alphanumeric to random entry')
                rappendentry = str(randomentry())
                extraentries['Email'] = extraentries.Email.map(str)
                extraentries['Email'] = extraentries['Email'].str.replace('[', rappendentry)
                logging.info('complete' + '\n')
                DisplayMessage('complete' + '\n')
                logging.info('Appending email tag')
                DisplayMessage('Appending email tag')
                extraentries['Email'] = extraentries['Email'].str.replace(']', '@boingboing.com')
                logging.info('complete' + '\n')
                DisplayMessage('complete' + '\n')
                logging.info('Creating Hash Column')
                DisplayMessage('Creating Hash Column')
                extraentries['Hash'] = extraentries['Email'].apply(lambda h: hashlib.sha512(h).hexdigest().upper().strip())
                logging.info('complete' + '\n')
                DisplayMessage('complete' + '\n')
                logging.info('Concatenating two dataframes')
                DisplayMessage('Concatenating two dataframes')
                cdf = pd.concat([ecsv, extraentries], ignore_index=True)
                logging.info('complete' + '\n')
                DisplayMessage('complete' + '\n')
                logging.info('Sorting by Hash')
                DisplayMessage('Sorting by Hash')
                adf = cdf.sort(['Hash'], ascending=True)
                logging.info('complete' + '\n')
                DisplayMessage('complete' + '\n')
                logging.info('Writing hash list to csv in ' + output)
                DisplayMessage('Writing hash list to csv in ' + output)
                results = ['Hash']
                adf.to_csv(output, sep=',', columns=results, header=False, index=False, encoding='utf-8')
                logging.info('complete' + '\n')
                DisplayMessage('complete' + '\n')
        else:
            logging.error('More than one column found ' + 'Number of columns = ' + ecol)
            DisplayMessage('More than one column found ' + 'Number of columns = ' + ecol)
    except:
        logging.error("Error")
        DisplayMessage("Error")


def pHashToEmail(elist, rHash, outfile):
    try:
        logging.info('')
        logging.info('Starting hash to email Function')
        print('Starting to make toast with email' + '\n')
        logging.info('Creating output file path')
        DisplayMessage('Creating output file path')
        output = os.path.join(outfile, "pHashToEmail.csv")
        logging.info('Complete' + '\n')
        DisplayMessage('Complete' + '\n')
        logging.info('Reading csv file into DataFrame')
        DisplayMessage('Reading csv file into DataFrame')
        coln = ['Email']
        ecsv = pd.read_csv(elist, names=coln)
        logging.info('Complete' + '\n')
        DisplayMessage('Complete' + '\n')
        logging.info('Storing the shape of the Dataframe')
        DisplayMessage('Storing the shape of the Dataframe')
        erows = str(ecsv.shape[0])
        ecol = str(ecsv.shape[1])
        logging.info('Complete' + '\n')
        DisplayMessage('Complete' + '\n')
        logging.info('Verifying there is only one column')
        DisplayMessage('Verifying there is only one column')
        if ecol > 1:
            logging.info('verified' + '\n')
            DisplayMessage('verified' + '\n')
            logging.info('Verifying that all csv rows contain emails')
            DisplayMessage('Verifying that all csv rows contain emails')
            cecsv = ecsv[ecsv['Email'].str.contains("@")]
            crows = str(cecsv.shape[0])
            if erows > crows:
                logging.warning(crows + ' emails found in ' + erows + ' rows' + '\n')
                DisplayMessage(crows + ' emails found in ' + erows + ' rows' + '\n')
                logging.info('Selecting only rows with emails')
                DisplayMessage('Selecting only rows with emails')
                common = cecsv.merge(ecsv, on=['Email'])
                logging.info('complete' + '\n')
                DisplayMessage('complete' + '\n')
                logging.info('Removing all white spaces and making all emails lower cased')
                DisplayMessage('Removing all white spaces and making all emails lower cased')
                common['Email'] = common['Email'].apply(lambda x: x.lower().strip())
                logging.info('complete' +'\n')
                DisplayMessage('complete' + '\n')
                logging.info('Creating sha512 hash id for each email string')
                DisplayMessage('Creating sha512 hash id for each email string')
                common['Hash'] = common['Email'].apply(lambda s: hashlib.sha512(s).hexdigest().upper().strip())
                logging.info('complete' + '\n')
                DisplayMessage('complete' + '\n')
                logging.info('Reading RSN hash csv file into DataFrame')
                DisplayMessage('Reading RSN hash csv file into DataFrame')
                coln2 = ['Hash']
                rhashcsv = pd.read_csv(rHash, names=coln2)
                logging.info('complete' + '\n')
                DisplayMessage('complete' + '\n')
                logging.info('Merging common partner hash rows to RSN hash rows')
                DisplayMessage('Merging common partner hash rows to RSN hash rows')
                pcommon = common.merge(rhashcsv, on=['Hash'])
                logging.info('complete' + '\n')
                DisplayMessage('complete' + '\n')
                logging.info('Writing email results to csv in ' + output)
                DisplayMessage('Writing email results to csv in ' + output)
                rcol = ['Email']
                pcommon.to_csv(output, sep=',', columns=rcol, header=False, index=False, encoding='utf-8')
                logging.info('complete' + '\n')
                DisplayMessage('complete' + '\n')
            else:
                logging.info('Verified' + '\n')
                DisplayMessage('Verified' + '\n')
                logging.info('Removing all white spaces and making all emails lower cased')
                DisplayMessage('Removing all white spaces and making all emails lower cased')
                ecsv['Email'] = ecsv['Email'].apply(lambda x: x.lower().strip())
                logging.info('complete' + '\n')
                DisplayMessage('complete' + '\n')
                logging.info('Creating sha512 hash id for each email string')
                DisplayMessage('Creating sha512 hash id for each email string')
                ecsv['Hash'] = ecsv['Email'].apply(lambda s: hashlib.sha512(s).hexdigest().upper().strip())
                logging.info('complete' + '\n')
                DisplayMessage('complete' + '\n')
                logging.info('Reading RSN hash csv file into DataFrame')
                DisplayMessage('Reading RSN hash csv file into DataFrame')
                coln2 = ['Hash']
                rhashcsv = pd.read_csv(rHash, names=coln2)
                logging.info('complete' + '\n')
                DisplayMessage('complete' + '\n')
                logging.info('Merging common partner hash rows to RSN hash rows')
                DisplayMessage('Merging common partner hash rows to RSN hash rows')
                common = ecsv.merge(rhashcsv, on=['Hash'])
                logging.info('complete' + '\n')
                DisplayMessage('complete' + '\n')
                logging.info('Writing email results to csv in ' + output)
                DisplayMessage('Writing email results to csv in ' + output)
                rcol = ['Email']
                common.to_csv(output, sep=',', columns=rcol, header=False, index=False, encoding='utf-8')
                logging.info('complete' + '\n')
                DisplayMessage('complete' + '\n')
        else:
            logging.error('More than one column found' + 'Number of columns = ' + ecol)
            DisplayMessage('More than one column found' + 'Number of columns = ' + ecol)
    except:
        logging.error("Failed to read in Client Hash List")
        DisplayMessage("Failed to read in Client Hash List")



# ------------ MAIN SCRIPT STARTS HERE -----------------
if __name__ == '__main__':
    #import datetime
    cdate = datetime.date.today()
    exp = datetime.date(2016, 7, 1)
    daystoexp = (exp - cdate)
    if daystoexp.days > 0:
        startTime = time.time()
        DWT_VERSION = '1.0.0'
        ReleaseDate = "April 11, 2016"

        spath = os.path.realpath(__file__)
        fpath = str(spath)
        lpath = fpath.replace('NonceTest.py', 'NonceTest.log')
        #lpath2 = (lpath + 'Toast.log')

        # Start your logging
        logging.basicConfig(filename=lpath, level=logging.DEBUG, format='%(asctime)s %(message)s')

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
        DisplayMessage("Program End")