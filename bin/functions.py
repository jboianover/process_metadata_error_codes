# -*- coding: utf-8 -*-
import getopt
import sys
import pandas as pd


def parse_arguments(arguments):
    try:
        opts, args = getopt.getopt(arguments, 'p:d:f:o:e:t:', ['process_name=', 'chdir=', 'file_name=', 'output_file=',
                                                               'chdir_ec=', 'file_ec='])

    except getopt.GetoptError as err:
        print(str(err))
        print('Options and Arguments:\n\
                -p | --process_name= Nombre del Proceso\n\
                -d | --chdir=        Nombre del PATH donde se ubica el process_metadata\n\
                -f | --file_name=    Nombre del archivo process_metadata\n\
                -o | --output_file=  Nombre del SQL de salida\n\
                -e | --chdir_ec=     Nombre del PATH donde se ubica el error_code_table\n\
                -t | --file_ec=      Nombre del archivo error_code_table\n')
        sys.exit()

    for k, v in opts:
        if k in ('-p', '--process_name'):
            process_name = v
        elif k in ('-d', '--chdir'):
            chdir = v
        elif k in ('-f', '--file_name'):
            file_name = v
        elif k in ('-o', '--output_file'):
            output_file = v
        elif k in ('-e', '--chdir_ec'):
            chdir_ec = v
        elif k in ('-t', '--file_ec'):
            file_ec = v

    return process_name, chdir, file_name, output_file, chdir_ec, file_ec


def check_non_equi(table_criteria):
    for seq in table_criteria:
        if '>' in seq or '>=' in seq or '<' in seq or '<=' in seq or '!=' in seq:
            return True
        else:
            return False


def check_non_equi_step_ahead(table_criteria_step_ahead):
    for seq in table_criteria_step_ahead:
        if '>' in seq or '>=' in seq or '<' in seq or '<=' in seq or '!=' in seq:
            return True
        else:
            return False


def header(file, batch, batches, process_table, build_part):
    if batch != max(batches):
        if build_part == 1:
            file.writelines(''.join(['drop table ', process_table, '_temp', str(batch), ';\n']))
            file.writelines(''.join(['create table ', process_table, '_temp', str(batch), ' as\n']))
        if build_part == 2:
            file.writelines(''.join(['insert into ', process_table, '_temp', str(batch), ' \n']))
    else:
        if build_part == 1:
            file.writelines(''.join(['drop table ', process_table, '_final;\n']))
            file.writelines(''.join(['create table ', process_table, '_final', ' as\n']))
        if build_part == 2:
            file.writelines(''.join(['insert into ', process_table, '_final', ' \n']))
    return 0


def select_clause(file, process_metadata, batch, batches, table_columns_reinyect, build_part,
                  require_hash_step_ahead_flg, dim_columns_step_ahead):
    special_default_values = ['date("1900-01-01")']
    dim_columns = process_metadata[['seq', 'value_custom_flg', 'value', 'value_alias', 'default', 'reinyectable']] \
        .drop_duplicates().reset_index(drop=True)
    dim_columns = dim_columns[pd.notnull(dim_columns['value'])]

    file.writelines(''.join(['select', '\n']))
    file.writelines(''.join(['t0.*']))
    if require_hash_step_ahead_flg:
        first_val = True
        file.writelines(''.join([',', '\n']))
        file.writelines(''.join(['md5(concat(']))
        for index, row in dim_columns_step_ahead.iterrows():
            if not first_val:
                file.writelines(''.join([', ']))
            if row['fk_custom_flg'] == 'N':
                file.writelines(''.join(['t0.', row['fk']]))
            else:
                file.writelines(''.join([row['fk']]))
            first_val = False
        file.writelines(''.join([')) hash_block_', str(batch)]))
    # SELECT Clause
    if build_part == 1:
        for index, row in dim_columns.iterrows():
            if row['value_custom_flg'] == 'N':
                if pd.isna(dim_columns.loc[index]['default']):
                    file.writelines(''.join([',', '\n']))
                    file.writelines(''.join(['t', str(row['seq']), '.', row['value']]))
                else:
                    file.writelines(''.join([',', '\n']))
                    if row['default'] not in special_default_values:
                        file.writelines(
                            ''.join(['case when', ' ', 't', str(row['seq']), '.', row['value'], ' ', 'is not null then',
                                     ' ',
                                     't', str(row['seq']), '.', row['value'], ' ', 'else', ' "', str(row['default']),
                                     '" ', 'end', ' ', row['value']]))
                    else:
                        file.writelines(
                            ''.join(['case when', ' ', 't', str(row['seq']), '.', row['value'], ' ', 'is not null then',
                                     ' ',
                                     't', str(row['seq']), '.', row['value'], ' ', 'else', ' ', str(row['default']),
                                     ' ', 'end', ' ', row['value']]))
            else:
                if pd.isna(dim_columns.loc[index]['default']):
                    file.writelines(''.join([',', '\n']))
                    file.writelines(' '.join([row['value'], row['value_alias']]))
                else:
                    if row['default'] not in special_default_values:
                        file.writelines(''.join([',', '\n']))
                        file.writelines(''.join(['case when (', row['value'], ') ', 'is not null then', ' (',
                                                 row['value'], ') else', ' "', str(row['default']),
                                                 '" ', 'end', ' ', row['value_alias']]))
                    else:
                        file.writelines(''.join([',', '\n']))
                        file.writelines(''.join(['case when (', row['value'], ') ', 'is not null then', ' (',
                                                 row['value'], ') else', ' ',
                                                 str(row['default']), ' ', 'end', ' ',
                                                 row['value_alias']]))

    if build_part == 2:
        for index, row in dim_columns.iterrows():
            if pd.isna(dim_columns.loc[index]['default']):
                file.writelines(''.join([',', '\n']))
                file.writelines(''.join(['null ', row['value']]))
            else:
                if row['default'] not in special_default_values:
                    file.writelines(''.join([',', '\n']))
                    file.writelines(''.join(['"', str(row['default']), '" ', row['value']]))
                else:
                    file.writelines(''.join([',', '\n']))
                    file.writelines(''.join(['', str(row['default']), ' ', row['value']]))

    # Reinyect & Error Codes
    if batch == max(batches):
        # Reinyect
        if build_part == 1:
            first_val = True
            file.writelines(''.join([',', '\n']))
            for index, row in table_columns_reinyect.iterrows():
                if not first_val:
                    file.writelines(''.join([' or ', '\n']))
                if row['seq'] == 0:
                    if row['value_custom_flg'] == 'N':
                        file.writelines(''.join(['case when', ' ', 't', str(row['seq']), '.', row['value'], ' ',
                                                 '= "', str(row['default']), '" then true else false end']))
                    elif row['value_custom_flg'] == 'Y':
                        file.writelines(''.join(['case when', ' ', 't', str(row['seq']), '.', row['value_alias'], ' ',
                                                 '= "', str(row['default']), '" then true else false end']))
                else:
                    if row['value_custom_flg'] == 'N':
                        file.writelines(''.join(['case when', ' ', 't', str(row['seq']), '.', row['value'], ' ',
                                                 'is null then true else false end']))
                    elif row['value_custom_flg'] == 'Y':
                        file.writelines(''.join(['case when (', row['value'], ') ',
                                                 '= "', str(row['default']), '" then true else false end']))
                first_val = False
            file.writelines(' reinyectable')
        if build_part == 2:
            file.writelines(''.join([',', '\n']))
            file.writelines(''.join(['true reinyectable']))
        # Error Codes
        if build_part == 1:
            first_val = True
            file.writelines(''.join([',', '\n']))
            file.writelines('concat(')
            file.writelines(''.join(['\n']))
            for index, row in table_columns_reinyect.iterrows():
                if not first_val:
                    file.writelines(''.join(['," else "" end,', '\n']))
                if row['seq'] == 0:
                    if row['value_custom_flg'] == 'N':
                        file.writelines(''.join(['case when', ' ', 't', str(row['seq']), '.', row['value'], ' ',
                                                 '= "', str(row['default']), '" then "', str(row['error_code'])]))

                    elif row['value_custom_flg'] == 'Y':
                        if pd.isna(dim_columns.loc[index]['default']):
                            file.writelines(
                                ''.join(['case when (', row['value'], ') is null then "', str(row['error_code'])]))

                        else:
                            file.writelines(
                                ''.join(['case when', ' ', 't', str(row['seq']), '.', row['value_alias'], ' ',
                                         '= "', str(row['default']), '" then "', str(row['error_code'])]))

                else:
                    if row['value_custom_flg'] == 'N':
                        file.writelines(''.join(['case when', ' ', 't', str(row['seq']), '.', row['value'], ' ',
                                                 'is null then "', str(row['error_code'])]))

                    elif row['value_custom_flg'] == 'Y':
                        file.writelines(
                            ''.join(['case when (', row['value'], ') is null then "', str(row['error_code'])]))

                first_val = False
            file.writelines('" else "" end) error_cd')
        if build_part == 2:
            first_val = True
            file.writelines(''.join([',', '\n']))
            file.writelines('concat(')
            file.writelines(''.join(['\n']))
            c = 1
            for index, row in table_columns_reinyect.iterrows():
                if not first_val and row['seq'] == 0:
                    file.writelines(''.join(['," else "" end,', '\n']))
                if row['seq'] == 0:
                    if row['value_custom_flg'] == 'N':
                        file.writelines(''.join(['case when', ' ', 't', str(row['seq']), '.', row['value'], ' ',
                                                 '= "', str(row['default']), '" then "', str(row['error_code'])]))
                        
                    elif row['value_custom_flg'] == 'Y':
                        if pd.isna(dim_columns.loc[index]['default']):
                            file.writelines(
                                ''.join(['case when (', row['value'], ') is null then "', str(row['error_code'])]))
                        else:
                            file.writelines(
                                ''.join(['case when', ' ', 't', str(row['seq']), '.', row['value_alias'], ' ',
                                         '= "', str(row['default']), '" then "', str(row['error_code'])]))
                    if c == len(table_columns_reinyect.loc[(table_columns_reinyect['seq'] == 0)]):
                        file.writelines(''.join(['," else "" end,']))
                else:
                    file.writelines(''.join(['\n']))
                    file.writelines(''.join(['"', str(row['error_code'])]))
                    if c != len(table_columns_reinyect):
                        file.writelines(''.join([',",']))
                    else:
                        file.writelines(''.join(['"']))
                first_val = False
                c += 1
            file.writelines(') error_cd')
    return 0


def from_clause(file, process_metadata, process_table_name, process_table, batches, build_part):
    if build_part == 1:
        dim_tables = process_metadata[['seq', 'dimension', 'how', 'criteria', 'type']].reset_index(drop=True)
        table_name = dim_tables[['seq', 'dimension']].drop_duplicates().groupby('seq')['dimension'].apply(list)
        table_how = dim_tables[['seq', 'how']].drop_duplicates().dropna().groupby('seq')['how'].apply(list)
        table_type = dim_tables[['seq', 'type']].drop_duplicates().dropna().groupby('seq')['type'].apply(list)
        table_criteria = dim_tables[['seq', 'criteria']].dropna().groupby('seq')['criteria'].apply(list)
        dim_tables = dim_tables.set_index('seq')

        dim_detail = process_metadata[['seq', 'key', 'fk', 'key_custom_flg', 'fk_custom_flg']] \
            .drop_duplicates().reset_index(drop=True)
        table_keys = dim_detail[['seq', 'key']].dropna().groupby('seq')['key'].apply(list)
        table_key_custom_flg = dim_detail[['seq', 'key_custom_flg']].dropna().groupby('seq')['key_custom_flg'].apply(
            list)
        table_fks = dim_detail[['seq', 'fk']].dropna().groupby('seq')['fk'].apply(list)
        table_fks_custom_flg = dim_detail[['seq', 'fk_custom_flg']].dropna().groupby('seq')['fk_custom_flg'].apply(list)

        file.writelines(''.join(['\n', 'from', '\n']))
        file.writelines(''.join([process_table_name, ' ', 't0']))
        for seq, table in table_name.iteritems():
            if table_type.loc[seq][0] == 'join':
                # Only One Condition JOIN
                if len(table_keys[seq]) == 1:
                    # Not Custom FK
                    if table_fks_custom_flg[seq][0] == 'N':
                        file.writelines(''.join(['\n', table_how[seq][0], ' join ', table[0], ' ', 't', str(seq)]))
                        # Is Equi Join
                        if table_criteria.loc[seq][0] == '=':
                            # Is Not Custom Key
                            if table_key_custom_flg[seq][0] == 'N':
                                file.writelines(
                                    ''.join([' on ', 't0.', table_fks[seq][0], ' ', table_criteria.loc[seq][0],
                                             ' ', 't', str(seq), '.', table_keys[seq][0]]))
                            # Is Custom Key
                            else:
                                file.writelines(
                                    ''.join([' on ', 't0.', table_fks[seq][0], ' ', table_criteria.loc[seq][0],
                                             ' ', table_keys[seq][0]]))
                    # Custom FK
                    else:
                        file.writelines(''.join(['\n', table_how[seq][0], ' join ', table[0], ' ', 't', str(seq)]))
                        # Is Equi Join
                        if table_criteria.loc[seq][0] == '=':
                            # Is Not Custom Key
                            if table_key_custom_flg[seq][0] == 'N':
                                file.writelines(
                                    ''.join([' on ', table_fks[seq][0], ' ', table_criteria.loc[seq][0], ' ',
                                             't', str(seq), '.', table_keys[seq][0]]))
                            # Is Custom Key
                            else:
                                file.writelines(
                                    ''.join([' on ', table_fks[seq][0], ' ', table_criteria.loc[seq][0], ' ',
                                             table_keys[seq][0]]))
                else:
                    # Multiple Conditions JOIN
                    for k in range(0, len(table_keys[seq])):
                        # First Condition
                        if k == 0:
                            # Not Custom FK
                            if table_fks_custom_flg[seq][k] == 'N':
                                file.writelines(
                                    ''.join(['\n', table_how[seq][0], ' join ', table[0], ' ', 't', str(seq)]))
                                # Is Equi Join
                                if table_criteria[seq][k] == '=':
                                    # Is Not Custom Key
                                    if table_key_custom_flg[seq][k] == 'N':
                                        file.writelines(
                                            ''.join([' on ', 't0.', table_fks[seq][k], ' ', table_criteria[seq][k],
                                                     ' ', 't', str(seq), '.', table_keys[seq][k]]))
                                    # Is Custom Key
                                    else:
                                        file.writelines(
                                            ''.join([' on ', 't0.', table_fks[seq][k], ' ', table_criteria[seq][k],
                                                     ' ', table_keys[seq][k]]))
                            else:
                                # Custom FK
                                file.writelines(
                                    ''.join(['\n', table_how[seq][0], ' join ', table[0], ' ', 't', str(seq)]))
                                # Is Equi Join
                                if table_fks_custom_flg[seq][k] == 'N':
                                    # Is Not Custom Key
                                    if table_key_custom_flg[seq][k] == 'N':
                                        file.writelines(
                                            ''.join([' on ', table_fks[seq][k], ' ', table_criteria[seq][k], ' ',
                                                     't', str(seq), '.', table_keys[seq][k]]))
                                    # Is Custom Key
                                    else:
                                        file.writelines(
                                            ''.join([' on ', table_fks[seq][k], ' ', table_criteria[seq][k], ' ',
                                                     table_keys[seq][k]]))
                        # Rest
                        else:
                            # Not Custom FK and Is Equi Join
                            if table_fks_custom_flg[seq][k] == 'N' and table_criteria.loc[seq][k] == '=':
                                # Is Not custom Key
                                if table_key_custom_flg[seq][k] == 'N':
                                    file.writelines(''.join(
                                        [' and ', 't0.', table_fks[seq][k], ' ', table_criteria.loc[seq][k], ' ',
                                         't', str(seq), '.', table_keys[seq][k]]))
                                # Is Custom Key
                                else:
                                    file.writelines(''.join(
                                        [' and ', 't0.', table_fks[seq][k], ' ', table_criteria.loc[seq][k], ' ',
                                         table_keys[seq][k]]))
                            # Custom FK and Is Equi Join
                            elif table_fks_custom_flg[seq][k] == 'Y' and table_criteria.loc[seq][k] == '=':
                                # Is Not Custom Key
                                if table_key_custom_flg[seq][k] == 'N':
                                    file.writelines(''.join(
                                        [' and ', table_fks[seq][k], ' ', table_criteria.loc[seq][k], ' ',
                                         't', str(seq), '.', table_keys[seq][k]]))
                                # Is Custom Key
                                else:
                                    file.writelines(''.join(
                                        [' and ', table_fks[seq][k], ' ', table_criteria.loc[seq][k], ' ',
                                         table_keys[seq][k]]))
            # Fixed JOIN
            elif table_type.loc[seq][0] == 'fixed':
                file.writelines(''.join(
                    ['\n', table_how[seq][0], ' join ', table[0], ' ', 't', str(seq), ' on ',
                     't', str(seq), '.', table_keys[seq][0], ' ', table_criteria.loc[seq][0]]))
    if build_part == 2:
        file.writelines(''.join(['\n']))
        file.writelines(''.join(['from ', process_table, '_temp', str(batches[len(batches) - 2]), ' t0']))
    return 0


def where_clause(file, process_metadata, build_part, process_table, batch, batches):
    if build_part == 1:
        first_val = 'Y'
        for index, row in process_metadata.iterrows():
            if row['type'] == 'join' and first_val == 'Y':
                if row['fk_custom_flg'] == 'N' and row['criteria'] != '=':
                    file.writelines(''.join(['\n', 'where', '\n']))
                    file.writelines(''.join(['t0.', row['fk'], ' ', row['criteria'],
                                             ' ', 't', str(row['seq']), '.', row['key']]))
                    first_val = 'N'
                elif row['fk_custom_flg'] == 'Y' and row['criteria'] != '=':
                    file.writelines(''.join(['\n', 'where', '\n']))
                    file.writelines(''.join([row['fk'], ' ', row['criteria'], ' ',
                                             't', str(row['seq']), '.', row['key']]))
                    first_val = 'N'
            elif row['type'] == 'join' and first_val == 'N':
                if row['fk_custom_flg'] == 'N' and row['criteria'] != '=':
                    file.writelines(''.join([' and ', '\n']))
                    file.writelines(''.join(['t0.', row['fk'], ' ', row['criteria'],
                                             ' ', 't', str(row['seq']), '.', row['key']]))
                elif row['fk_custom_flg'] == 'Y' and row['criteria'] != '=':
                    file.writelines(''.join([' and ', '\n']))
                    file.writelines(''.join([row['fk'], ' ', row['criteria'], ' ',
                                             't', str(row['seq']), '.', row['key']]))
        file.writelines(''.join([';\n\n']))
    if build_part == 2:
        if batch == max(batches):
            process_table_name = ''.join([process_table, '_final'])
        else:
            process_table_name = ''.join([process_table, '_temp', str(batch)])
        file.writelines(''.join(['\n']))
        file.writelines(''.join(
            ['where t0.hash_block_', str(batch - 1), ' not in (select distinct hash_block_', str(batch - 1), ' from ',
             process_table_name, ')']))
        file.writelines(''.join([';\n\n']))
    return 0


def join_pm_ec(pm, df_ec):
    # filtro los error codes activos
    df_activo = df_ec.loc[df_ec['activo'] == 'Y']

    # left join de process_metadata con error_codes activos por dimension,value
    df_merge = pd.merge(pm, df_activo, how='left', left_on=['dimension', 'value'], right_on=['tabla', 'campo'])

    # update del error_code origen con el de error_codes si lo encontre y no tiene custom_flg
    df_merge.loc[pd.notna(df_merge['error_code_y']) & (df_merge['value_custom_flg'] == 'N'), ['error_code_x']] = \
        df_merge['error_code_y']

    # renombrado de error_code y version para que queden los nombres originales post-join
    df_merge.rename(columns={'error_code_x': 'error_code', 'version_x': 'version'}, inplace=True)

    # filtro las columnas deseadas y elimino las repetidas
    df_merge = df_merge[
        ['process', 'table', 'version', 'seq', 'batch', 'active', 'dimension', 'key', 'key_custom_flg',
         'fk_custom_flg', 'fk', 'value_custom_flg', 'value', 'value_alias', 'default', 'type', 'how',
         'criteria', 'reinyectable', 'error_code', 'hash_block']]

    # genero otro process_metadata solamente con los que tienen custom_flg y alias
    pm_alias = pm.loc[(pm['value_alias'].notnull()) & (pm['value_custom_flg'] == 'Y')]

    # left join del process_metadarta de alias con los error_code activos
    df_merge_alias = pd.merge(pm_alias, df_activo, how='left', left_on=['dimension', 'value_alias'],
                              right_on=['tabla', 'campo'])

    # renombrado de error_code y version para mantener nombres originales post-join
    df_merge_alias.rename(columns={'error_code_x': 'error_code', 'version_x': 'version'}, inplace=True)

    # update de error_code por el encontrado en error_codes activos
    df_merge_alias.loc[pd.notna(df_merge_alias['error_code_y']), ['error_code']] = df_merge_alias['error_code_y']

    # filtro los campos del resultado del join de los custom values con su error code
    df_merge_alias = df_merge_alias[['dimension', 'value_custom_flg', 'value', 'value_alias', 'error_code']]

    # renombro las columnas para que queden con su nombre original
    df_merge_alias.rename(columns={'dimension': 'dimension_y', 'value_custom_flg': 'value_custom_flg_y'}, inplace=True)

    df_merge_alias.rename(columns={'value': 'value_y', 'value_alias': 'value_alias_y', 'error_code': 'error_code_y'},
                          inplace=True)

    # join final entre los no custom con sus error_code y los custom con sus error_code
    df_merge_final = pd.merge(df_merge, df_merge_alias, how='left', left_on=['dimension', 'value_alias'],
                              right_on=['dimension_y', 'value_alias_y'])

    # me quedo con el error code del custom cuando es  custom
    df_merge_final.loc[
        pd.notna(df_merge_final['error_code_y']) & (df_merge_final['value_custom_flg'] == 'Y'), ['error_code']] = \
        df_merge_final['error_code_y']

    # renombro las columnas para que queden con su nombre original
    df_merge_final.rename(columns={'process_x': 'process', 'table_x': 'table', 'version_x': 'version', 'seq_x': 'seq'},
                          inplace=True)

    df_merge_final.rename(columns={'batch_x': 'batch', 'active_x': 'active', 'dimension': 'dimension', 'key_x': 'key'},
                          inplace=True)

    df_merge_final.rename(
        columns={'key_custom_flg_x': 'key_custom_flg'}, inplace=True)

    df_merge_final.rename(
        columns={'fk_custom_flg_x': 'fk_custom_flg', 'fk_x': 'fk', 'value_custom_flg_x': 'value_custom_flg',
                 'value_x': 'value'}, inplace=True)

    df_merge_final.rename(
        columns={'value_alias': 'value_alias', 'default_x': 'default', 'type_x': 'type', 'how_x': 'how'}, inplace=True)

    df_merge_final.rename(
        columns={'criteria_x': 'criteria', 'reinyectable_x': 'reinyectable', 'error_code_x': 'error_code',
                 'hash_block_x': 'hash_block'}, inplace=True)

    return df_merge_final.reset_index(drop=True)
