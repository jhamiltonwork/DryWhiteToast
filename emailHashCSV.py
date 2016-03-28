import csv
import hashlib
import multiprocessing


def ReadWrite(efile, filewrite):
    with open(efile, 'rb') as csvfile:
            with open(filewrite, 'w') as outputfile:
                reader = csv.reader(csvfile, delimiter=',', quotechar='|')
                writer = csv.writer(outputfile)
                for row in (reader):
                    rowstring = str(row)
                    rowstring = '\n'.join(row)
                    rowstring = rowstring.lower()
                    rowstring = rowstring.strip()
                    stringhash = Hashing(rowstring)
#                    hashlist = list(stringhash)
                    writer.writerow([stringhash])

    csvfile.close()
    outputfile.close()

def Hashing(lines):
    for line in lines:
        hashes = hashlib.sha256(lines.encode('utf-8')).hexdigest()
        hashes = hashes.upper()
#        hashSplit = ','.join(hashes)
        return hashes


emailList = ('EmailTest.csv')
output = ('Hashed.csv')
rowsInFile = ReadWrite(emailList, output)

#------------ MAIN SCRIPT STARTS HERE -----------------
if __name__ == '__main__':
    emailsProcessed = multiprocessing.Process(target=ReadWrite(emailList, output))
    emailsProcessed.start()