import pandas as pd
import re
import yaml


class UnknownActionError(Exception):
    pass


class ActionList:
    def __init__(self, list_of_actions):
        self.actions = []
        for step in list_of_actions:
            action_name = list(step.keys())[0]
            action = Action.factory(action_name, step[action_name])
            self.actions.append(action)

    def perform_instructions(self, input_data):
        output_data = input_data
        for action in self.actions:
            output_data = action.perform_instructions(output_data)
        return output_data


# abstract, never used.
class Action:
    def __init__(self, instructions):
        self.instructions = instructions

    @staticmethod
    def factory(action, instruction):
        # just calls different constructors based on passed action
        action_classes = {
            'change_date_or_time_format': ChangeDateFormat,
            'copy': CopyAction,
            'rename': RenameAction,
            'extract_query_string': ExtractQueryStringAction,
            'extract_text': ExtractTextAction,
            'filter_columns': FilterColumnAction,
            'filter_rows': FilterRowAction,
            'edit_specific_rows': FilteredRowFormulaAction,
            'formula': FormulaAction,
            'remove_columns': RemoveColumnAction,
            'remove_duplicates': RemoveDuplicatesAction,
            'replace_text': ReplaceTextAction,
            'append_text': AppendTextAction,
            'prepend_text': PrependTextAction,
            'sum_up_by': GroupBySumAction,
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
            format_date = lambda x: x.strftime(instruction['date_format'])
            dates = input_data[instruction['target_column']].dropna()
            input_data[result_col] = dates.map(format_date)
        return input_data


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


class FilteredRowFormulaAction(Action):
    """
    self.instructions: list of dict
        keys:
            filter_row
            formula
    e.g.
        - filter_rows: 1 < b < 3
          formula: a = 666
    """
    def perform_instructions(self, input_data):
        output_data = input_data
        for instruction in self.instructions:
            filter_instruction = instruction['filter_rows']
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
            except ValueError:
                # These hacks exists as .eval only works with numbers, not text
                result_col, _, value = instruction.split(maxsplit=2)
                if value[0] in ['"', "'"]:
                    # set to a string
                    input_data[result_col] = value[1:-1]
                else:
                    input_data[result_col] = input_data[value]
        return input_data
