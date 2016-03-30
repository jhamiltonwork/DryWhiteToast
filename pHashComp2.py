import csv
import hashlib
import multiprocessing


def ReadWrite(efile, filewrite, rfile):
    with open(efile, 'rb') as csvfile:
        with open(rfile, 'rb') as rhashfile:
            with open(filewrite, 'w') as outputfile:
                rnreader = csv.reader(rhashfile, delimiter=',', quotechar='|')
                reader = csv.reader(csvfile, delimiter=',', quotechar='|')
                writer = csv.writer(outputfile, delimiter=',', quotechar='|', quoting=csv.QUOTE_MINIMAL)
                for row in (reader):
                    rowstring = str(row)
                    rowstring = '\n'.join(row)
                    rowstring = rowstring.lower()
                    rowstring = rowstring.strip()
                    stringhash = Hashing(rowstring)
                    sendrnlist = ReadRhash(rnreader)
                    resToDict = makeDictionary(rowstring, stringhash)
                    #                    cleanRlist = [x for x in sendrnlist if x is not None]
                    #                    print cleanRlist
                    cleanTuple = compareHash(resToDict, sendrnlist, writer)
                    #                    print(cleanTuple)

    csvfile.close()
    rhashfile.close()
    outputfile.close()


def Hashing(lines):
    for line in lines:
        hashes = hashlib.sha256(lines.encode('utf-8')).hexdigest()
        hashes = hashes.upper()
        #        hashSplit = ','.join(hashes)
        return hashes


def ReadRhash(rnhashlist):
    for liners in rnhashlist:
        #        listers = tuple(liners)
        return liners


def makeDictionary(emailp, hashp):
    rdictionary = {}
    rdictionary[emailp] = hashp
    return rdictionary


def compareHash(pDict, rhashList, filewriter):
    cleanedNoneTypeList = []
    if rhashList:
        cleanedNoneTypeList = cleanList(rhashList)
    else:
        pass
    cset = set(cleanedNoneTypeList)
    for key, value in pDict.items():
        if cset and value[0] in cset:
            filewriter.writerow([key])


def cleanList(receiveList):
    for lines in receiveList:
        if receiveList:
            return lines
        break



emailList = ('EmailTest.csv')
output = ('pHashComp2Emailresult.csv')
rhashlist = ('rHashed.csv')
# rowsInFile = ReadWrite(emailList, output, rhashlist)
# rnHashList = ReadRHash(rhashlist)


# ------------ MAIN SCRIPT STARTS HERE -----------------
if __name__ == '__main__':
    emailsProcessed = multiprocessing.Process(target=ReadWrite(emailList, output, rhashlist))
    emailsProcessed.start()
