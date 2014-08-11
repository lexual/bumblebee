import pandas as pd
import re
import yaml


class UnknownActionError(Exception):
    pass


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

        self.list_of_actions = list_of_actions or []

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

    def transform(self, csv_filename):
        output_data = self.extract(csv_filename)
        for step in self.list_of_actions:
            action_name = list(step.keys())[0]
            action = action_factory(action_name, step[action_name])
            output_data = action.perform_instructions(output_data)
        return output_data

    def extract(self, csv_filename):
        kwargs = {
            'filepath_or_buffer': csv_filename,
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


def action_factory(action, instruction):
    # just calls different constructors based on passed action
    action_classes = {
        'change_date_or_time_format': ChangeDateFormat,
        'copy': CopyAction,
        'rename': RenameAction,
        'extract_query_string': ExtractQueryStringAction,
        'extract_text': ExtractTextAction,
        'filter_columns': FilterColumnAction,
        'filter_rows': FilterRowAction,
        'formula': FormulaAction,
        'remove_columns': RemoveColumnAction,
        'remove_duplicates': RemoveDuplicatesAction,
        'sum_up_by': GroupBySumAction,
    }
    try:
        action_class = action_classes[action]
        return action_class(instruction)
    except KeyError:
        msg = 'action {} unknown'.format(action)
        raise UnknownActionError(msg)


# abstract, never used.
class Action:
    def __init__(self, instructions):
        self.instructions = instructions


class GroupBySumAction(Action):
    """
    self.instructions: list of columns to group by.
    """
    def perform_instructions(self, input_data):
        columns_to_group_by = self.instructions
        grouped = input_data.groupby(columns_to_group_by, as_index=False)
        output_data = grouped.sum()
        return output_data


class ChangeDateFormat(Action):
    """
    self.instructions: list of dicts
        keys:
            date_or_time_column
            result_column
            date_format
    """
    def perform_instructions(self, input_data):
        for instruction in self.instructions:
            result_col = instruction['result_column']
            format_date = lambda x: x.strftime(instruction['date_format'])
            dates = input_data[instruction['date_or_time_column']].dropna()
            input_data[result_col] = dates.map(format_date)
        return input_data


class ExtractTextAction(Action):
    """
    self.instructions: dict
    keys:
        text_column
        column_to_store_extract
        regex
    """
    def perform_instructions(self, input_data):
        for instruction in self.instructions:
            text_col = instruction['text_column']
            result_col = instruction['column_to_store_extract']
            regex = instruction['regex']
            text = input_data[text_col]
            input_data[result_col] = text.str.extract(regex, re.VERBOSE)
        return input_data


class ExtractQueryStringAction(Action):
    """
    self.instructions: list of dicts
    keys:
        column_to_store_extract
        query_string
        url_column
    """
    def perform_instructions(self, input_data):
        regex_instructions = []
        for instruction in self.instructions:
            result_col = instruction['column_to_store_extract']
            regex = """
                .*          # anything at the start
                [?&]        # query string prececed by & or ?
                {} =         # extract x query string.
                ([^&#]*)    # capture up until & or #
            """.format(instruction['query_string'])
            regex_instruction = {
                'text_column': instruction['url_column'],
                'column_to_store_extract': result_col,
                'regex': regex,
            }
            regex_instructions.append(regex_instruction)
        a = ExtractTextAction(regex_instructions)
        return a.perform_instructions(input_data)


class FilterRowAction(Action):
    """
    self.instructions: list of strings with query
    e.g.
    'a < 2'
    '10 < temp < 20'
    """
    def perform_instructions(self, input_data):
        for instruction in self.instructions:
            input_data = input_data.query(instruction)
        return input_data


class FilterColumnAction(Action):
    """
    self.instructions: list of columns to keep
    """
    def perform_instructions(self, input_data):
        output_data = input_data.loc[:, self.instructions]
        return output_data


class RemoveColumnAction(Action):
    """
    self.instructions: list of columns to drop
    """
    def perform_instructions(self, input_data):
        output_data = input_data.drop(self.instructions, axis='columns')
        return output_data


class RemoveDuplicatesAction(Action):
    """
    self.instructions: list of columns to drop duplicate values
    """
    def perform_instructions(self, input_data):
        output_data = input_data.drop_duplicates(self.instructions)
        return output_data


class RenameAction(Action):
    """
    self.instructions: list of strings of form:
    'new_column_name = old_column_name'
    """
    def perform_instructions(self, input_data):
        renames = {}
        for instruction in self.instructions:
            new_col, old_col = instruction.split('=')
            new_col, old_col = new_col.strip(), old_col.strip()
            renames[old_col] = new_col
        output_data = input_data.rename(columns=renames)
        return output_data


class CopyAction(Action):
    """
    self.instructions: list of strings of form:
    'new_column_name = old_column_name'
    """
    def perform_instructions(self, input_data):
        # TODO: refactor out 2 common lines? worth it?
        for instruction in self.instructions:
            new_col, old_col = instruction.split('=')
            new_col, old_col = new_col.strip(), old_col.strip()
            input_data[new_col] = input_data[old_col]
        return input_data


class FormulaAction(Action):
    """
    self.instructions: list of strings representing computation
    e.g.
    'fahrenheit = celsius * 9 / 5 + 32'
    """
    def perform_instructions(self, input_data):
        for instruction in self.instructions:
            try:
                input_data.eval(instruction)
            except ValueError:
                # These hacks exists as .eval only works with numbers, not text
                result_col, _, value = instruction.split(maxsplit=2)
                if value[0] in ['"', "'"]:
                    # set to a string
                    input_data[result_col] = value[1:-1]
                else:
                    input_data[result_col] = input_data[value]
        return input_data
