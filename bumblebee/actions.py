import pandas as pd
import re
import yaml


class UnknownActionError(Exception):
    pass


class ActionList:
    def __init__(self, list_of_actions):
        self.actions = []
        for step in list_of_actions:
            try:
                action_name = list(step.keys())[0]
                instruction = step[action_name]
            except AttributeError:
                action_name = step
                instruction = []
            action = Action.factory(action_name, instruction)
            self.actions.append(action)

    def perform_instructions(self, input_data):
        output_data = input_data
        for action in self.actions:
            output_data = action.perform_instructions(output_data)
        return output_data

    @classmethod
    def from_yaml(cls, filepath_or_buffer):
        if isinstance(filepath_or_buffer, str):
            with open(filepath_or_buffer) as f:
                actions = yaml.safe_load(f)
        else:
            actions = yaml.safe_load(filepath_or_buffer)
        return cls(actions)


# abstract, never used.
class Action:
    def __init__(self, instructions):
        self.instructions = instructions

    @staticmethod
    def factory(action, instruction):
        # just calls different constructors based on passed action
        action_classes = {
            'change_date_or_time_format': ChangeDateFormat,
            'copy_column': CopyAction,
            'rename_column': RenameAction,
            'extract_query_string': ExtractQueryStringAction,
            'extract_text': ExtractTextAction,
            'only_keep_these_columns': FilterColumnAction,
            'only_keep_rows_where': FilterRowAction,
            'only_edit_rows_where': EditSpecificRowsAction,
            'run_these_formula': FormulaAction,
            'remove_columns': RemoveColumnAction,
            'remove_duplicates': RemoveDuplicatesAction,
            'replace_text': ReplaceTextAction,
            'add_text_at_end': AppendTextAction,
            'add_text_at_start': PrependTextAction,
            'sum_up_by': GroupBySumAction,
            'make_column_names_lowercase': LowerCaseColumnNamesAction,
            'make_column_names_alphanumeric': AlphaNumColumnNamesAction,
            'ensure_column_is_in_this_format': ChangeColumnFormatAction,
        }
        try:
            action_class = action_classes[action]
            return action_class(instruction)
        except KeyError:
            msg = 'action {} unknown'.format(action)
            raise UnknownActionError(msg)


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
            target_column
            result_column
            date_format
    """
    def perform_instructions(self, input_data):
        for instruction in self.instructions:
            result_col = instruction['result_column']
            date_format = instruction['date_format']
            date_format_strings = (
                # TODO: hour/minute/second support.
                ('YYYY', '%Y'),
                ('YY', '%y'),
                ('MM', '%m'),
                ('DD', '%d'),
            )
            for date_str, strformat in date_format_strings:
                date_format = date_format.replace(date_str, strformat)
                date_format = date_format.replace(date_str.lower(), strformat)
            format_date = lambda x: x.strftime(date_format)
            dates = input_data[instruction['target_column']].dropna()
            input_data[result_col] = dates.map(format_date)
        return input_data


class AlphaNumColumnNamesAction(Action):
    """
    """
    def perform_instructions(self, input_data):
        spaces_to_underscore = lambda x: x.replace(' ', '_')
        output_data = input_data.rename(columns=spaces_to_underscore)
        strip_punctuation = lambda x: re.sub('[^\w_]', '', x)
        output_data = output_data.rename(columns=strip_punctuation)
        return output_data


class LowerCaseColumnNamesAction(Action):
    """
    """
    def perform_instructions(self, input_data):
        output_data = input_data.rename(columns=str.lower)
        return output_data


class ChangeColumnFormatAction(Action):
    """
    self.instructions: list of dicts
    """
    def perform_instructions(self, input_data):
        output_data = input_data
        for instruction in self.instructions:
            column_format = list(instruction.values())[0]
            column = list(instruction.keys())[0]
            if column_format == 'date':
                output_data[column] = pd.to_datetime(output_data[column])
            if column_format == 'text':
                output_data[column] = output_data[column].fillna('')
                output_data[column] = output_data[column].dropna().astype(int)
                output_data[column] = output_data[column].dropna().astype(str)

        return output_data


class AppendTextAction(Action):
    """
    self.instructions: list of dicts
        keys:
            target_column
            result_column
            text
    """
    def perform_instructions(self, input_data):
        output_data = input_data
        for instruction in self.instructions:
            col = instruction['target_column']
            text = instruction['text']
            result_col = instruction['result_column']
            # TODO: factor out commonality between Prepend/Append
            output_data[result_col] = output_data[col] + text
        return output_data


class PrependTextAction(Action):
    """
    self.instructions: list of dicts
        keys:
            target_column
            result_column
            text
    """
    def perform_instructions(self, input_data):
        output_data = input_data
        for instruction in self.instructions:
            col = instruction['target_column']
            text = instruction['text']
            result_col = instruction['result_column']
            output_data[result_col] = text + output_data[col]
        return output_data


class ReplaceTextAction(Action):
    """
    self.instructions: list of dicts
        keys:
            target_column
            result_column
            text_to_find
            replacement_text
    """
    def perform_instructions(self, input_data):
        output_data = input_data
        for instruction in self.instructions:
            col = instruction['target_column']
            replacement = instruction['replacement_text']
            text_to_find = instruction['text_to_find']
            if text_to_find == '^':
                result = replacement + output_data[col]
            elif text_to_find == '$':
                result = output_data[col] + replacement
            else:
                result = output_data[col].str.replace(text_to_find,
                                                      replacement)
            result_col = instruction['result_column']
            output_data[result_col] = result
        return output_data


class ExtractTextAction(Action):
    """
    self.instructions: dict
    keys:
        target_column
        result_column
        regex
    """
    def perform_instructions(self, input_data):
        for instruction in self.instructions:
            text_col = instruction['target_column']
            result_col = instruction['result_column']
            regex = instruction['regex']
            text = input_data[text_col]
            input_data[result_col] = text.str.extract(regex, re.VERBOSE)
        return input_data


class ExtractQueryStringAction(Action):
    """
    self.instructions: list of dicts
    keys:
        result_column
        query_string
        target_column
    """
    def perform_instructions(self, input_data):
        regex_instructions = []
        for instruction in self.instructions:
            result_col = instruction['result_column']
            regex = """
                .*          # anything at the start
                [?&]        # query string prececed by & or ?
                {} =        # extract x query string.
                ([^&#]*)    # capture up until & or #
            """.format(instruction['query_string'])
            regex_instruction = {
                'target_column': instruction['target_column'],
                'result_column': result_col,
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


class EditSpecificRowsAction(Action):
    """
    self.instructions: list of dict
        keys:
            filter_row
            run_these_formula
    e.g.
        - rows_match: 1 < b < 3
          run_these_formula: a = 666
    """
    def perform_instructions(self, input_data):
        output_data = input_data
        for instruction in self.instructions:
            filter_instruction = instruction['rows_match']
            filter_row_action = FilterRowAction([filter_instruction])
            filtered_data = filter_row_action.perform_instructions(output_data)
            actions = ActionList(instruction['list_of_actions'])
            transformed = actions.perform_instructions(filtered_data)
            output_data.loc[transformed.index] = transformed
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
            except (ValueError, SyntaxError):
                # These hacks exists as .eval only works with numbers, not text
                result_col, value = instruction.split(' = ', maxsplit=1)
                if value[0] in ['"', "'"]:
                    # set to a string
                    input_data[result_col] = value[1:-1]
                else:
                    try:
                        input_data[result_col] = input_data[value]
                    except KeyError:
                        input_data[result_col] = value
        return input_data
