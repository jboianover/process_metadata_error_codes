# -*- coding: utf-8 -*-

import pandas as pd
import os
import sys
sys.path.insert(0, os.path.join(os.getcwd(), 'bin'))
from functions import header, select_clause, from_clause, where_clause, check_non_equi, \
check_non_equi_step_ahead, join_pm_ec, parse_arguments

pd.options.display.max_rows = 999

# Sample Arguments #
#arg = ['-p', 'trafico_actuaciones_f_tr_actuacion_detallada',
#       '-d', 'C:\\Users\\Maximiliano Bloisen\\Desktop\\altamira\\py',
#       '-f', 'process_metadata_actuaciones.xlsx',
#       '-o', 'process.sql',
#       '-e', 'C:\\Users\\Jonathan Boianover\\Desktop',
#       '-t', 'error_code_table.csv'
#      ]

#process_name, chdir, file_name, output_file = parse_arguments(arg)

process_name, chdir, file_name, output_file, chdir_ec, file_ec = parse_arguments(sys.argv[1:])

#chdir_ec = 'C:\\Users\\Jonathan Boianover\\Desktop'
#file_ec = 'error_code_table.csv'

pm_raw = pd.read_excel(os.path.join(chdir, file_name), sheet_name=0, header=0, names=None, index_col=None,
                                 convert_float=True, converters={'error_code': str})


pm_filtered = pm_raw.loc[(pm_raw['process'] == process_name) & (pm_raw['version'] == max(pm_raw['version'])) &
                         (pm_raw['active'] == 'Y')]

df_ec = pd.read_csv(os.path.join(chdir_ec, file_ec), delimiter=',', names=['tabla', 'campo', 'error_code', 'version',
                                                                           'activo', 'fecha_insercion'], skiprows=1)

pm = join_pm_ec(pm_filtered, df_ec)

#python3
#file = open(os.path.join(chdir, output_file), 'w', encoding='utf8')
#python2
file = open(os.path.join(chdir, output_file), 'w')

#pm = pm.loc[(pm['process'] == process_name) & (pm['version'] == max(pm['version'])) & (pm['active'] == 'Y')]


process_table = pm[['table']].drop_duplicates().reset_index(drop=True)['table'][0]

batches = pm['batch'].drop_duplicates().astype(int).tolist()

first_batch_flg = 'Y'

table_columns_reinyect = pm.loc[(pm['reinyectable'] == 'Y')]\
    [['batch', 'seq', 'value_custom_flg', 'value', 'value_alias', 'default', 'error_code']]
table_columns_reinyect.loc[table_columns_reinyect['batch'] != max(batches), ['seq']] = 0
table_columns_reinyect = table_columns_reinyect[['seq', 'value_custom_flg', 'value', 'value_alias', 'default', 'error_code']]

for batch in batches:
    process_metadata = pm.loc[(pm['batch'] == batch)]
    dim_tables = process_metadata[['seq', 'dimension', 'how', 'criteria', 'type']].reset_index(drop=True)
    table_criteria = dim_tables[['seq', 'criteria']].dropna().groupby('seq')['criteria'].apply(list)

    require_hash_flg = check_non_equi(table_criteria)

    require_hash_step_ahead_flg = False
    if batch != max(batches):
        process_metadata_step_ahead = pm.loc[(pm['batch'] == batch + 1)]
        dim_tables_step_ahead = process_metadata_step_ahead[['seq', 'criteria']].reset_index(drop=True)
        table_criteria_step_ahead = dim_tables_step_ahead[['seq', 'criteria']].dropna().groupby('seq')[
            'criteria'].apply(list)
        require_hash_step_ahead_flg = check_non_equi_step_ahead(table_criteria_step_ahead)

    dim_columns_step_ahead = []
    if require_hash_step_ahead_flg:
        dim_columns_step_ahead = process_metadata_step_ahead.loc[(process_metadata_step_ahead['hash_block'] == 'Y')][
            ['fk_custom_flg', 'fk']]
    print("Batch: ", batch, " of ", max(batches),
          " - Reinyect: ", batch == max(batches),
          " - Require Hash: ", require_hash_step_ahead_flg)
    build_parts = 1
    if require_hash_flg:
        build_parts = 2

    for build_part in range(1, build_parts + 1):
        # HEADER
        header(file, batch, batches, process_table, build_part)

        # SELECT
        select_clause(file, process_metadata, batch, batches, table_columns_reinyect, build_part,
                      require_hash_step_ahead_flg, dim_columns_step_ahead)

        # FROM
        if first_batch_flg == 'Y':
            process_table_name = process_table
        first_batch_flg = 'N'
        from_clause(file, process_metadata, process_table_name, process_table, batches, build_part)
        process_table_name = ''.join([process_table, '_temp', str(batch)])

        # WHERE
        where_clause(file, process_metadata, build_part, process_table, batch, batches)
