import csv
import sys
import pprint

if __name__ == '__main__':
    filename = sys.argv[1]
    with open(filename, 'r') as csvfile:
        row_reader = csv.reader(csvfile, delimiter=',', quotechar='"')
        headers = next(row_reader)
        # print(headers)
        lookup = dict(zip(headers, range(0, len(headers))))
        print(lookup)
        # pprint.pprint(lookup)
