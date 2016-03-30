# --------------DryWhiteToast ver. 1.1.0
# Built by 'James Robert Cameron Hamilton' handle='Felix','537i'
#
#
# import hashlib  # Python Standard Library - Secure hashes and message digests
import csv  # Python Standard Library - reader and writer for csv files
import multiprocessing
import pandas


def HashCompare(pHash, rHash, outFile):
    phashcsv  = pandas.read_csv(pHash, header=0)
    column_a = list(phashcsv.a)
    print column_a
#    with open(pHash, 'rb') as pFile:
#        with open(rHash, 'rb') as RNfile:
#            with open(outFile, 'w') as output:
#                writer = csv.writer(output, delimiter=',', quotechar='|', quoting=csv.QUOTE_MINIMAL)
#                preader = csv.reader(pFile, delimiter=',', quotechar='|')
#                rreader = csv.reader(RNfile, delimiter=',', quotechar='|')
#                for elements in rreader:
#                    rhashes = str(elements)
#                    rhashes = '\n'.join(elements)
#                    rhashes = rhashes.strip()
#                    rhashes = rhashes.upper()
#                    phashes = ReadRows(preader)
#                    comparefile = compareHash(phashes, rhashes, writer)
#    pFile.close()
#    RNfile.close()
#    output.close()

def ReadRows(files):
    for lines in (files):
        hashstring = str(lines)
        hashstring = '\n'.join(lines)
        hashstring = hashstring.strip()
        hashstring = hashstring.upper()
        return hashstring


def compareHash(plister, rlister, writefile):
    partList = list(plister)
    rnhashlist = list(rlister)
    if partList:
        cleanedNoneList = cleanList(partList)
    else:
        pass
#    print (cleanedNoneList)
#    for elements in partList:
#        if elements not in rnhashlist:
#            print elements

#    for key, value in pDict.items():
#        if cset and value[0] in cset:
#            filewriter.writerow([key])

def cleanList(receiveList):
    for lines in receiveList:
        if receiveList:
            print lines
        break

rCSV = ('Hashed.csv')
pCSV = ('pHashCompareTester.csv')
oCSV = ('RNhashReturn.csv')

# ------------ MAIN SCRIPT STARTS HERE -----------------
if __name__ == '__main__':
    hashEliminator = multiprocessing.Process(target=HashCompare(pCSV, rCSV, oCSV))
    hashEliminator.start()
