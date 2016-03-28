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
                    listsend = CompareHash([rowstring], [stringhash], sendrnlist)
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
        listers = list(liners)
        return listers


def CompareHash(emailp, hashp, rfile):
    result = (hashp == rfile)
    if result is True:
        result2 = (emailp, result)
        print result2[0]











emailList = ('/home/ragnar/PycharmProjects/DryWhiteToast/EmailTest.csv')
output = ('/home/ragnar/PycharmProjects/DryWhiteToast/Hashed.csv')
rhashlist = ('/home/ragnar/PycharmProjects/DryWhiteToast/ReturnedEmailCompare.csv')
rowsInFile = ReadWrite(emailList, output, rhashlist)
# rnHashList = ReadRHash(rhashlist)


# ------------ MAIN SCRIPT STARTS HERE -----------------
# if __name__ == '__main__':
#    emailsProcessed = multiprocessing.Process(target=ReadWrite(emailList, output, rhashlist))
#    emailsProcessed.start()
