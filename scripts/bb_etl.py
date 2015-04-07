#!/usr/bin/env python

from __future__ import print_function

import bumblebee
import bumblebee as bb
from StringIO import StringIO
import sys

usage = """
HELP
bb_etl.py input.csv transformation_rules.yaml > output.csv
"""

def main():
    try:
        args = sys.argv[1:]
        input_csv, transformation_file = args
    except:
        print(usage)
        return
    convertor = bb.Convertor.from_yaml(transformation_file)
    output = convertor.transform(input_csv)
    output_buffer = StringIO()
    output.to_csv(output_buffer, index=False)
    output_buffer.seek(0)
    print(output_buffer.read(), end='')


if __name__ == '__main__':
    main()
