#!/usr/bin/env python

import bumblebee
import bumblebee as bb
import sys

usage = """
HELP
bb_etl.py input.csv transformation_rules.yaml output.csv
"""

def main():
    try:
        args = sys.argv[1:]
        input_csv, transformation_file, output_csv = args
    except:
        print usage
        return
    convertor = bb.Convertor.from_yaml(transformation_file)
    output = convertor.transform(input_csv)
    output.to_csv(output_csv, index=False)
    print 'Transformation successful!'


if __name__ == '__main__':
    main()
