import numpy as np
import pandas as pd
import re
import six
import yaml

from .actions import ActionList


class Transformer:
    def __init__(self,
                 data_format='csv',
                 column_headers_are_on_row_number=1,
                 number_of_rows_to_skip_at_file_end=0,
                 encoding='utf-8',
                 read_these_columns_in_these_formats=None,
                 only_load_these_columns=None,
                 column_separator=',',
                 read_from_row_that_starts_with=None,
                 list_of_actions=None):
        self.data_format = data_format
        self.encoding = encoding
        self.read_from_row_that_starts_with = read_from_row_that_starts_with
        self.column_separator = column_separator
        column_formats = read_these_columns_in_these_formats or {}
        self.read_these_columns_in_these_formats = column_formats
        self.only_load_these_columns = only_load_these_columns or []

        list_of_actions = list_of_actions or []
        self.action_list = ActionList(list_of_actions)

        header_row = column_headers_are_on_row_number
        self.column_headers_are_on_row_number = header_row
        nrows = number_of_rows_to_skip_at_file_end
        self.number_of_rows_to_skip_at_file_end = nrows

    @classmethod
    def from_yaml(cls, filepath_or_buffer):
        if isinstance(filepath_or_buffer, str):
            with open(filepath_or_buffer) as f:
                config = yaml.safe_load(f)
        else:
            config = yaml.safe_load(filepath_or_buffer)
        kwargs = {}
        options = [
            'column_separator',
            'encoding',
            'column_headers_are_on_row_number',
            'number_of_rows_to_skip_at_file_end',
            'read_these_columns_in_these_formats',
            'only_load_these_columns',
            'read_from_row_that_starts_with',
            'list_of_actions',
        ]
        for option in options:
            if option in config:
                kwargs[option] = config[option]
        o = cls(**kwargs)
        return o

    def transform(self, filepath_or_buffer):
        input_data = self.extract(filepath_or_buffer)
        output_data = self.action_list.perform_instructions(input_data)
        return output_data

    def extract(self, filepath_or_buffer):
        kwargs = {
            'filepath_or_buffer': filepath_or_buffer,
            'skiprows': self.column_headers_are_on_row_number - 1,
            'infer_datetime_format': True,
            'sep': self.column_separator,
            'encoding': self.encoding,
            'dayfirst': True,
        }
        if self.read_these_columns_in_these_formats:
            if 'date' in self.read_these_columns_in_these_formats:
                date_cols = self.read_these_columns_in_these_formats['date']
                kwargs['parse_dates'] = date_cols
            if 'text' in self.read_these_columns_in_these_formats:
                text_cols = self.read_these_columns_in_these_formats['text']
                kwargs['dtype'] = {col: str for col in text_cols}

        if self.only_load_these_columns:
            kwargs['usecols'] = self.only_load_these_columns
        if self.read_from_row_that_starts_with:
            row_start = self.read_from_row_that_starts_with
            header_row = _find_line_number_starting_with(filepath_or_buffer,
                                                         row_start)
            kwargs['skiprows'] = header_row - 1

        input_data = pd.read_csv(**kwargs)
        # c engine doesn't support skipfooter, so we'll do manually.
        if self.number_of_rows_to_skip_at_file_end:
            end_slice = -self.number_of_rows_to_skip_at_file_end
            input_data = input_data.iloc[:end_slice]
        if 'number' in self.read_these_columns_in_these_formats:
            for col in self.read_these_columns_in_these_formats['number']:
                dtype = input_data[col].dtype
                if (not np.issubdtype(dtype, int) and
                        not np.issubdtype(dtype, float)):
                    input_data[col] = input_data[col].str.replace(',', '')
                    input_data[col] = input_data[col].str.replace('$', '')
                    try:
                        input_data[col] = input_data[col].astype(int)
                    except ValueError:
                        input_data[col] = input_data[col].astype(float)
        return input_data


def _find_line_number_starting_with(filepath_or_buffer, text):
    passed_filename = isinstance(filepath_or_buffer, six.string_types)
    if passed_filename:
        f = open(filepath_or_buffer)
    else:
        f = filepath_or_buffer

    line_number = None
    for i, line in enumerate(f, 1):
        if line.startswith(text):
            line_number = i
    if passed_filename:
        f.close()
    else:
        f.seek(0)
    return line_number
