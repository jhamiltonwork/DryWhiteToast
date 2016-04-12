import logging
import pandas as pd
import hashlib
import multiprocessing
import time
import sys
import os
import argparse
import datetime
import re
import uuid
import socket

def ParseCommandLine():
    logging.info('Starting ParseCommandLine' + '\n')
    parser = argparse.ArgumentParser('...Dry White Toast')

    group = parser.add_mutually_exclusive_group()
    group.add_argument('-v', "--verbose", help="allows progress messages to be displayed", action='store_true')

    # setup arguments
    parser.add_argument('-e', '--emailList', required=False,
                        help="specify the path and name of the csv with the email list for hashing")
    parser.add_argument('-r', '--reportPath', type=ValidateDirectoryWritable, required=True,
                        help="specify the folder/directory where the report will be saved")
    parser.add_argument('-c', '--clientHash', required=False,
                        help="specify the path and csv name of the client hash list")
    parser.add_argument('-m', '--rsnHashMatch', required=False,
                        help="specify the RSN csv hash list to match to")

    # create a global object to hold the validated arguments, these will be available then

    global gl_args
    global gl_verbose

    gl_args = parser.parse_args()

    logging.info('Verifying which CLI arguments are selected')

    if gl_args.verbose:
        gl_verbose = True
        logging.info('verbose selected')
    else:
        gl_verbose = False
    if gl_args.emailList:
        logging.info('email to hash function selected')
        ReadWrite(gl_args.emailList, gl_args.reportPath)
    else:
        gl_args.emailList = False
    if gl_args.rsnHashMatch:
        logging.info('hash matching function selected')
        ReadMergeCompare(gl_args.clientHash, gl_args.rsnHashMatch, gl_args.reportPath)
    else:
        gl_args.clientHash = False
        gl_args.rsnHashMatch = False


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


def ReadWrite(elist, outfile):
    try:
        logging.info('')
        logging.info('Starting ReadWrite Function')
        print('Starting to make toast with email' + '\n')
        logging.info('Creating output file path')
        DisplayMessage('Creating output file path')
        output = os.path.join(outfile, "Hashed.csv")
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
                logging.info('Writing hash list to csv in ' + output)
                DisplayMessage('Writing hash list to csv in ' + output)
                results = ['Hash']
                common.to_csv(output, sep=',', columns=results, header=False, index=False, encoding='utf-8')
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
                logging.info('Writing hash list to csv in ' + output)
                DisplayMessage('Writing hash list to csv in ' + output)
                results = ['Hash']
                ecsv.to_csv(output, sep=',', columns=results, header=False, index=False, encoding='utf-8')
                logging.info('complete' + '\n')
                DisplayMessage('complete' + '\n')
        else:
            logging.error('More than one column found' + 'Number of columns = ' + ecol)
            DisplayMessage('More than one column found' + 'Number of columns = ' + ecol)
    except:
        logging.error("Failed to read in Client Hash List")
        DisplayMessage("Failed to read in Client Hash List")


def ReadMergeCompare(cHash, rHash, outfile):
    try:
        logging.info('')
        logging.info('Starting hash compare function')
        DisplayMessage('Starting hash compare function')
        logging.info('Creating output file path')
        DisplayMessage('Creating output file path')
        output = os.path.join(outfile, "comparedListToReturn.csv")
        logging.info('complete' + '\n')
        DisplayMessage('complete' + '\n')
        logging.info('Reading partner hash csv file into DataFrame')
        DisplayMessage('Reading partner hash csv file into DataFrame')
        coln = ['Hash']
        phashcsv = pd.read_csv(cHash, names=coln)
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
        common = phashcsv.merge(rhashcsv, on=['Hash'])
        logging.info('complete' + '\n')
        DisplayMessage('complete' + '\n')
        logging.info('Removing matching partner hash rows')
        DisplayMessage('Removing matching partner hash rows')
        results = phashcsv[(~phashcsv.Hash.isin(common.Hash))]
        logging.info('complete' + '\n')
        DisplayMessage('complete' + '\n')
        logging.info('Writing hash list to csv in ' + output)
        DisplayMessage('Writing hash list to csv in ' + output)
        rcol = ['Hash']
        results.to_csv(output, sep=',', columns=rcol, header=False, index=False, encoding='utf-8')
        logging.info('complete' + '\n')
        DisplayMessage('complete' + '\n')
    except:
        logging.error("Failed to read in hash list")
        DisplayMessage("Failed to read in hash lists")


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
        DisplayMessage("Program End")
