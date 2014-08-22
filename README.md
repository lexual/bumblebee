Bumblebee
=========

Write ETL descriptions in YAML. Should be trivial to extend to JSON.

Just a thought-bubble at the moment, but the idea is:

* Readable by anyone, including non-programmers.
* Create a simple webapp with GUI to create this YAML.

e.g. this dataset: http://www.bom.gov.au/fwo/IDV60901/IDV60901.94868.axf

    read_from_row_that_starts_with: 19
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

Then Python calls are:

    import bumblebee as bb

    t = bb.Transformer.from_yaml('etl.yaml')
    output = t.transform('my_data.csv')

Gives:

                name                time  air_temp  air_temp_f  apparent_t
        0  Melbourne 2014-08-08 22:00:00      10.1       50.18         6.3
        1  Melbourne 2014-08-08 21:30:00      10.5       50.90         7.8
        2  Melbourne 2014-08-08 21:00:00      10.9       51.62         8.8


Current Operations:
* change_date_or_time_format
* copy_column
* rename_column
* extract_query_string
* extract_text
* only_keep_these_columns
* only_keep_rows_where
* only_edit_rows_where
* run_these_formula
* remove_columns
* remove_duplicates
* replace_text
* add_text_at_end
* add_text_at_start
* sum_up_by
* make_column_names_lowercase
* make_column_names_alphanumeric
* ensure_column_is_in_this_format

Supports Python 2 & 3.
