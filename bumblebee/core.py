import pandas as pd
import re
import yaml

from .actions import ActionList


class Transformer:
    def __init__(self,
                 data_format='csv',
                 number_of_rows_to_skip_at_file_start=0,
                 number_of_rows_to_skip_at_file_end=0,
                 encoding='utf-8',
                 columns_with_dates_or_times=None,
                 only_load_these_columns=None,
                 column_separator=',',
                 list_of_actions=None):
        self.data_format = data_format
        self.encoding = encoding
        self.column_separator = column_separator
        self.columns_with_dates_or_times = columns_with_dates_or_times or []
        self.only_load_these_columns = only_load_these_columns or []

        list_of_actions = list_of_actions or []
        self.action_list = ActionList(list_of_actions)

        nrows = number_of_rows_to_skip_at_file_start
        self.number_of_rows_to_skip_at_file_start = nrows
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
            'number_of_rows_to_skip_at_file_start',
            'number_of_rows_to_skip_at_file_end',
            'columns_with_dates_or_times',
            'only_load_these_columns',
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
            'skiprows': self.number_of_rows_to_skip_at_file_start,
            'parse_dates': self.columns_with_dates_or_times,
            'infer_datetime_format': True,
            'sep': self.column_separator,
            'encoding': self.encoding,
            'dayfirst': True,
        }
        if self.only_load_these_columns:
            kwargs['usecols'] = self.only_load_these_columns

        input_data = pd.read_csv(**kwargs)
        # c engine doesn't support skipfooter, so we'll do manually.
        if self.number_of_rows_to_skip_at_file_end:
            end_slice = -self.number_of_rows_to_skip_at_file_end
            input_data = input_data.iloc[:end_slice]
        return input_data
