# --------------DryWhiteToast ver. 1.1.0
# Built by 'James Robert Cameron Hamilton' handle='Felix','537i'
#
#
# import hashlib  # Python Standard Library - Secure hashes and message digests
import csv  # Python Standard Library - reader and writer for csv files
import multiprocessing


def HashCompare():
    with open('RNemailHashComparePartTest.csv', 'rb') as pFile:
        with open('RNemailHashCompareRSNTest.csv', 'rb') as RNfile:
            with open('RNhashReturn.csv', 'w') as output:
                writer = csv.writer(output, delimiter=',')
                preader = csv.reader(pFile)
                rreader = csv.reader(RNfile)
                plist = list(preader)
                rlist = list(rreader)
                for element in plist:
                    if element not in rlist:
                        assert isinstance(element, list)
                        writer.writerow(element)


# ------------ MAIN SCRIPT STARTS HERE -----------------
if __name__ == '__main__':
    hashEliminator = multiprocessing.Process(target=HashCompare)
    hashEliminator.start()
