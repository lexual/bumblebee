# import io
import math
import os
import pandas as pd

from six import StringIO
from .context import Transformer


class TestTransformation:
    def setup(self):
        self.testdatadir = os.path.join(os.path.dirname(__file__), 'data')
        self.test_csv_input = os.path.join(self.testdatadir,
                                           'example_data.csv')

    def test_rename_column(self):
        yaml_config = """
            list_of_actions:
                - rename_column:
                    - full_name = name
        """
        output = self._run_transformation(yaml_config, self.test_csv_input)
        assert 'full_name' in output
        assert 'name' not in output
        assert len(output) == 2
        print('\n', output)

    def test_copy_column(self):
        yaml_config = """
            list_of_actions:
                - copy_column:
                    - full_name = name
        """
        output = self._run_transformation(yaml_config, self.test_csv_input)
        assert 'full_name' in output
        assert 'name' in output
        assert len(output) == 2
        # TODO: don't explicitly depend on Pandas in the test?!?
        pd.util.testing.assert_series_equal(output['full_name'],
                                            output['name'])

    def test_specify_column_separator(self):
        yaml_config = """
            column_separator: '\t'
        """
        test_csv = os.path.join(self.testdatadir, 'data_cols.tsv')
        output = self._run_transformation(yaml_config, test_csv)
        assert len(output) == 2
        assert 'a' in output

    def test_make_columns_lowercase(self):
        yaml_config = """
            list_of_actions:
                - make_column_names_lowercase
        """
        test_csv = os.path.join(self.testdatadir, 'data_funky_columns.csv')
        output = self._run_transformation(yaml_config, test_csv)
        assert 'first name' in output
        assert 'First Name' not in output

    def test_make_columns_alphanumeric(self):
        yaml_config = """
            list_of_actions:
                - make_column_names_alphanumeric
        """
        test_csv = os.path.join(self.testdatadir, 'data_funky_columns.csv')
        output = self._run_transformation(yaml_config, test_csv)
        assert 'First Name' not in output
        assert 'First_Name' in output
        assert 'Weight in kgs.' not in output
        assert 'Weight_in_kgs' in output

    def test_specify_encoding(self):
        yaml_config = """
            encoding: utf-16
        """
        test_csv = os.path.join(self.testdatadir, 'utf16.csv')
        output = self._run_transformation(yaml_config, test_csv)
        assert len(output) == 2
        assert 'a' in output

    def test_skip_rows_at_start(self):
        yaml_config = """
            column_headers_are_on_row_number: 4
        """
        test_csv = os.path.join(self.testdatadir, '3_extra_rows_at_start.csv')
        output = self._run_transformation(yaml_config, test_csv)
        assert len(output) == 2

    def test_skip_rows_at_end(self):
        yaml_config = """
            number_of_rows_to_skip_at_file_end: 3
        """
        test_csv = os.path.join(self.testdatadir, '3_extra_rows_at_end.csv')
        output = self._run_transformation(yaml_config, test_csv)
        assert len(output) == 2

    def test_skip_to_column_headers(self):
        yaml_config = """
            read_from_row_that_starts_with: name
        """
        test_csv = os.path.join(self.testdatadir, '3_extra_rows_at_start.csv')
        output = self._run_transformation(yaml_config, test_csv)
        assert len(output) == 2

    def test_skip_to_column_headers_with_buffer(self):
        yaml_config = """
            read_from_row_that_starts_with: name
        """
        test_csv = os.path.join(self.testdatadir, '3_extra_rows_at_start.csv')
        with open(test_csv) as f:
            output = self._run_transformation(yaml_config, f)
        assert len(output) == 2

    def test_parsing_numbers_as_text_action(self):
        yaml_config = """
            list_of_actions:
                - ensure_column_is_in_this_format:
                    - temp: text
        """
        test_csv = os.path.join(self.testdatadir, 'data_dates.csv')
        output = self._run_transformation(yaml_config, test_csv)
        assert len(output) == 2
        assert output['temp'][0] == '30'

    def test_parsing_date_column_action(self):
        yaml_config = """
            list_of_actions:
                - ensure_column_is_in_this_format:
                    - date: date
                    - time: date
                    - wordy_date: date
        """
        test_csv = os.path.join(self.testdatadir, 'data_dates.csv')
        output = self._run_transformation(yaml_config, test_csv)
        assert len(output) == 2
        assert output['date'][0].year == 2014
        assert output['time'][0].year == 2014
        assert output['wordy_date'][0].year == 2014

    def test_parsing_date_column(self):
        yaml_config = """
            read_these_columns_in_these_formats:
                date:
                    - date
                    - time
                    - wordy_date
        """
        test_csv = os.path.join(self.testdatadir, 'data_dates.csv')
        output = self._run_transformation(yaml_config, test_csv)
        assert len(output) == 2
        assert output['date'][0].year == 2014
        assert output['time'][0].year == 2014
        assert output['wordy_date'][0].year == 2014

    def test_parsing_numbers_as_text(self):
        yaml_config = """
            read_these_columns_in_these_formats:
                text:
                    - a
        """
        test_csv = os.path.join(self.testdatadir, 'data_cols.csv')
        output = self._run_transformation(yaml_config, test_csv)
        assert len(output) == 2
        assert output['a'][0] == '1'
        assert output['b'][0] == 1

    def test_parsing_numbers_with_columns(self):
        yaml_config = """
            read_these_columns_in_these_formats:
                number:
                    - d
                    - e
                    - f
        """
        test_csv = os.path.join(self.testdatadir, 'data_cols.csv')
        output = self._run_transformation(yaml_config, test_csv)
        assert len(output) == 2
        assert output['d'][0] == 1001
        assert output['e'][0] == 2
        assert output['f'][0] == 3.5


    def test_only_load_certain_columns(self):
        yaml_config = """
            only_load_these_columns:
                - b
                - c
                - d
        """
        test_csv = os.path.join(self.testdatadir, 'data_cols.csv')
        output = self._run_transformation(yaml_config, test_csv)
        assert len(output) == 2
        assert 'a' not in output
        assert 'b' in output
        assert 'c' in output
        assert 'd' in output
        assert 'e' not in output

    def test_column_addition(self):
        yaml_config = """
            list_of_actions:
                - run_these_formula:
                    - sum = a + b
        """
        test_csv = os.path.join(self.testdatadir, 'data_cols.csv')
        output = self._run_transformation(yaml_config, test_csv)
        assert len(output) == 2
        assert 'sum' in output
        assert output['sum'][0] == 2

    def test_column_set_to_text(self):
        yaml_config = """
            list_of_actions:
                - run_these_formula:
                    - foo = 'bar'
        """
        test_csv = os.path.join(self.testdatadir, 'data_cols.csv')
        output = self._run_transformation(yaml_config, test_csv)
        assert len(output) == 2
        assert 'foo' in output
        assert output['foo'][0] == 'bar'

    def test_substitute_text(self):
        yaml_config = """
            list_of_actions:
                - replace_text:
                    - target_column: client
                      result_column: result
                      text_to_find: f
                      replacement_text: F
        """
        test_csv = os.path.join(self.testdatadir, 'data_group.csv')
        output = self._run_transformation(yaml_config, test_csv)
        assert len(output) == 5
        assert 'result' in output
        assert output['result'][0] == 'Foo'


    def test_prepend_text(self):
        yaml_config = """
            list_of_actions:
                - add_text_at_start:
                    - target_column: client
                      result_column: result
                      text: hi-
        """
        test_csv = os.path.join(self.testdatadir, 'data_group.csv')
        output = self._run_transformation(yaml_config, test_csv)
        assert len(output) == 5
        assert 'result' in output
        assert output['result'][0] == 'hi-foo'

    def test_append_text(self):
        yaml_config = """
            list_of_actions:
                - add_text_at_end:
                    - target_column: client
                      result_column: result
                      text: -bar
        """
        test_csv = os.path.join(self.testdatadir, 'data_group.csv')
        output = self._run_transformation(yaml_config, test_csv)
        assert len(output) == 5
        assert 'result' in output
        assert output['result'][0] == 'foo-bar'

    def test_filter_columns(self):
        yaml_config = """
            list_of_actions:
                - only_keep_these_columns:
                    - a
                    - b
        """
        test_csv = os.path.join(self.testdatadir, 'data_filter.csv')
        output = self._run_transformation(yaml_config, test_csv)
        assert len(output) == 3
        assert 'a' in output
        assert 'b' in output
        assert 'c' not in output

    def test_filter_columns_with_spaces(self):
        yaml_config = """
            list_of_actions:
                - copy_column:
                    - adam ant = a
                - only_keep_these_columns:
                    - adam ant
                    - b
        """
        test_csv = os.path.join(self.testdatadir, 'data_filter.csv')
        output = self._run_transformation(yaml_config, test_csv)
        assert len(output) == 3
        assert 'adam ant' in output
        assert 'b' in output
        assert 'c' not in output

    def test_remove_columns(self):
        yaml_config = """
            list_of_actions:
                - remove_columns:
                    - a
                    - b
        """
        test_csv = os.path.join(self.testdatadir, 'data_filter.csv')
        output = self._run_transformation(yaml_config, test_csv)
        assert len(output) == 3
        assert 'a' not in output
        assert 'b' not in output
        assert 'c' in output

    def test_filter_rows(self):
        yaml_config = """
            list_of_actions:
                - only_keep_rows_where:
                    - 1 < b < 3
        """
        test_csv = os.path.join(self.testdatadir, 'data_filter.csv')
        output = self._run_transformation(yaml_config, test_csv)
        assert len(output) == 1
        assert output['b'].values[0] == 2

    def test_filtered_formula(self):
        yaml_config = """
            list_of_actions:
                - only_edit_rows_where:
                    - rows_match: 1 < b < 3
                      list_of_actions:
                        - run_these_formula:
                            - a = 666
        """
        test_csv = os.path.join(self.testdatadir, 'data_filter.csv')
        output = self._run_transformation(yaml_config, test_csv)
        assert len(output) == 3
        assert output['a'].values[0] == 1
        assert output['a'].values[1] == 666

    def test_drop_duplicates(self):
        yaml_config = """
            list_of_actions:
                - remove_duplicates:
                    - date
                    - client
        """
        test_csv = os.path.join(self.testdatadir, 'data_group.csv')
        output = self._run_transformation(yaml_config, test_csv)
        assert len(output) == 3

    def test_output_date_in_particular_format(self):
        yaml_config = """
            read_these_columns_in_these_formats:
                date:
                    - date
            list_of_actions:
                - change_date_or_time_format:
                    - target_column: date
                      result_column: date_string_y
                      date_format: YYYY
                - change_date_or_time_format:
                    - target_column: date
                      result_column: date_string
                      date_format: mm/dd/YYYY
                - change_date_or_time_format:
                    - target_column: date
                      result_column: date_string_x
                      date_format: mm/dd/YY
        """
        test_csv = os.path.join(self.testdatadir, 'data_dates.csv')
        output = self._run_transformation(yaml_config, test_csv)
        assert len(output) == 2
        print('\n', output)
        assert output['date_string_y'][0] == '2014'
        assert output['date_string'][0] == '01/13/2014'
        assert output['date_string_x'][0] == '01/13/14'

    def test_regex_extraction(self):
        yaml_config = """
            list_of_actions:
                - rename_column:
                    - time = local_date_time_full[80]
                - extract_text:
                    - target_column: url
                      result_column: x_value
                      regex: |
                          # Note re.VERBOSE is default.
                          .*          # anything at the start
                          [?&]        # query string prececed by & or ?
                          x =         # extract x query string.
                          ([^&#]*)    # capture up until & or #
        """
        test_csv = os.path.join(self.testdatadir, 'data_urls.csv')
        output = self._run_transformation(yaml_config, test_csv)
        assert len(output) == 6
        assert 'x_value' in output
        assert all(output['x_value'][:4] == 'foo')
        assert math.isnan(output['x_value'][4])
        assert math.isnan(output['x_value'][5])

    def test_query_string_extraction(self):
        yaml_config = """
            list_of_actions:
                - extract_query_string:
                    - target_column: url
                      result_column: x_value
                      query_string: x
        """
        test_csv = os.path.join(self.testdatadir, 'data_urls.csv')
        output = self._run_transformation(yaml_config, test_csv)
        assert len(output) == 6
        assert 'x_value' in output
        assert all(output['x_value'][:4] == 'foo')
        assert math.isnan(output['x_value'][4])
        assert math.isnan(output['x_value'][5])

    def test_summed_group(self):
        yaml_config = """
            list_of_actions:
                - sum_up_by:
                    - date
                    - client
        """
        test_csv = os.path.join(self.testdatadir, 'data_group.csv')
        output = self._run_transformation(yaml_config, test_csv)
        assert len(output) == 3
        v = output[(output.date == '2014-01-01') & (output.client == 'foo')]
        assert v.a[0] == 3
        assert v.b[0] == 4

    def test_melbourne_weather(self):
        yaml_config = """
            column_headers_are_on_row_number: 20
            number_of_rows_to_skip_at_file_end: 2
            read_these_columns_in_these_formats:
                date:
                    - local_date_time_full[80]
            list_of_actions:
                - rename_column:
                    - time = local_date_time_full[80]
                    - name = name[80]
                - run_these_formula:
                    - air_temp_f = air_temp * 9 / 5 + 32
                - only_keep_rows_where:
                    - apparent_t < 10 & name == 'Melbourne'
                - only_keep_these_columns:
                    - name
                    - time
                    - air_temp
                    - air_temp_f
                    - apparent_t
        """
        test_csv = os.path.join(self.testdatadir, 'melb_weather.csv')
        output = self._run_transformation(yaml_config, test_csv)
        assert len(output) > 0
        assert 'apparent_t' in output
        print()
        print(output.iloc[:3])
        print(output.iloc[-3:])

    def _run_transformation(self, yaml_text, test_csvpath):
        config = StringIO(yaml_text)
        t = Transformer.from_yaml(config)
        output = t.transform(test_csvpath)
        return output
