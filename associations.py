# -*- coding: utf-8 -*-
"""
Created on Wed May  1 11:59:46 2019

@author: lfontenelle
"""

#from Python_Data_Function import *
# Put package imports here
# Please make sure you have the correct packages installed in your Python environment
# -*- coding: utf-8 -*-
import xlrd
import pyodbc
import os
import datetime
import pandas as pd
import sqlalchemy as sa
from tkinter import Tk
from tkinter.filedialog import askopenfilename


engine = sa.create_engine("mssql+pyodbc://soghou2k12-1/Spotfire_Reporting_OnPrem?driver=SQL+Server+Native+Client+11.0")


Tk().withdraw()


def reload_parameters_table():
    table_query = '''SELECT  *
    FROM [Spotfire_Reporting_OnPrem].[fcst].[Saved_TypeCurveSet_Params]
    where isActive = 1'''
    tableValues = pd.read_sql(table_query, engine)
    return tableValues


def check_sheet_names(filename):
    xls = xlrd.open_workbook(filename, on_demand=True)
    sheetNames= xls.sheet_names()
    expectedSheetNames = ['2019Q1_TC', 'Sheet1']
    SheetNameCheck = [sheet for sheet in sheetNames if sheet in expectedSheetNames]
    if len(SheetNameCheck) == 0:
        return 0
    else:
        return SheetNameCheck[0]

def check_parameters_table():
    curve_id_query = '''SELECT
        [SetName]
      ,[TypeCurveName]
      , 1 as isPresent
    FROM [Spotfire_Reporting_OnPrem].[fcst].[Saved_TypeCurveSet_Params]
    where isActive = 1'''
    curve_ids = pd.read_sql(curve_id_query, engine)
    return curve_ids


def set_params_to_inactive(dup_curve_dict):
    current_user = os.getlogin()
    for dup_curveKey, dupcurveValue in dup_curve_dict.items():
        mssql = pyodbc.connect('Driver={SQL Server}',
                               Server='soghou2k12-1',
                               Database='Spotfire_Reporting_OnPrem',
                               Trusted_Connection='yes',
                               Autocommit=True)
        cursor = mssql.cursor()
        cursor.execute('''update
        [Spotfire_Reporting_OnPrem].[fcst].[Saved_TypeCurveSet_Params]
        SET
        updated_at = ?,
        updated_by = ?,
        isActive   = 0
        where typeCurveName = ? and SetName = ? and
        created_at = (SELECT max(created_at) FROM
        [Spotfire_Reporting_OnPrem].[fcst].[Saved_TypeCurveSet_Params]
        where typeCurveName = ? and SetName = ? and isActive = 1)''',
                       operation_time, current_user, dup_curveKey,
                       dupcurveValue,  dup_curveKey,
                       dupcurveValue)
        mssql.commit()


def new_record_metadata(data):
    data['created_by'] = current_user
    data['created_at'] = operation_time
    data['isActive'] = 1
    return data


def update_record_metadata(data):
    data['updated_by'] = os.getlogin()
    data['updated_at'] = datetime.datetime.now()
    data['isActive'] = 0
    return data


def add_zeros_to_optional_fields(data):
    current_cols = list(data)

    alt_cols = {'Mul_Oil1': 1, 'Mul_Oil2': 1, 'Mul_Oil3': 1,
                'Mul_Gas1': 1, 'Mul_Gas2': 1, 'Mul_Gas3': 1,
                'Oil_D_min': 0, 'Gas_D_min': 0, 'Water_D_min': 0,
                'Oil_AbdnRate': 0, 'Gas_AbdnRate': 0,
                'Water_AbdnRate': 0}
    missing_cols = [mcol for mcol in alt_cols if mcol not in current_cols]
    pop_cols = [pcol for pcol in alt_cols if pcol in current_cols]
    [alt_cols.pop(rm_col) for rm_col in pop_cols]

    if len(missing_cols) > 0:
        data = data.join(pd.DataFrame(alt_cols, index=data.index))
        return data
    else:
        return data


def range_check(data):
    decline_values = {'corOil_Decline': 'Oil_Decline',
                      'corGas_Decline': 'Gas_Decline',
                      'corWater_Decline': 'Water_Decline'}
    for correct, incoming in decline_values.items():
        data[correct] = (data[incoming]
                         .apply(lambda x: x/100 if x >= 1 else x))
        data.drop(columns=incoming, inplace=True)
        data.rename(index=str, columns={correct: incoming}, inplace=True)
    return data


def get_parameters_table():
    expected_columns = ['SetName', 'TypeCurveName', 'TCFromDailyRateName',
                        'Oil_IP', 'Oil_B', 'Oil_Decline', 'Gas_IP', 'Gas_B',
                        'Gas_Decline', 'Water_IP', 'Water_B', 'Water_Decline',
                        'DaysToHydrocarbon', 'DaysToPeak', 'OilRampAverage',
                        'GasRampAverage', 'DaysFlatWater', 'WaterFlatRate',
                        'Comment',
                        'Mul_Oil1', 'Mul_Oil2', 'Mul_Oil3',
                        'Mul_Gas1', 'Mul_Gas2', 'Mul_Gas3',
                        'Oil_D_min', 'Gas_D_min', 'Water_D_min',
                        'Oil_AbdnRate', 'Gas_AbdnRate', 'Water_AbdnRate']

    col_error_message = ''' was not the column name(s) that we were
                            expecting '''
    filename = askopenfilename()
    sheetName = check_sheet_names(filename)
    parameters_table = pd.read_excel(filename,
                                     sheet_name=sheetName,
                                     skiprows=0)
    in_columns = list(parameters_table)
    error_cols = [col for col in in_columns if col not in expected_columns]

    if len(error_cols) > 0:

        raise ValueError(','.join(error_cols) + col_error_message)
        return (','.join(error_cols) + col_error_message)
    else:
        add_fields = add_zeros_to_optional_fields(parameters_table)
        dataTable = range_check(add_fields)
        return dataTable


# method to write any values to the database.
def write_to_database(to_database):
    no_action = '''The curves provided are already in the system so none were
                 added'''
    if to_database.empty is not True:
        to_database.to_sql('Saved_TypeCurveSet_Params',
                           engine, schema='fcst',
                           if_exists='append',
                           index=False, chunksize=100)
        print(str(len(to_database.index)) + ' record(s) inserted')
        return str(len(to_database.index)) + ' record(s) inserted'
    else:
        print(no_action)
        return no_action


# main method for importing new parameters into the saved typecurve parms table
def update_parameters_table():
    id_col = 'TypeCurveName'
    parameters_table = get_parameters_table()
    curve_ids = check_parameters_table()

# check for duplicated in the database against the incoming table
    dups = pd.merge(curve_ids, parameters_table,
                    left_on=id_col,
                    right_on=id_col)
    dup_curves = dups[id_col].tolist()
    dup_setNames = dups['SetName_x'].tolist()
    dup_curve_dict = dict(zip(dup_curves, dup_setNames))
# update old parameters from active to  to inactive
    set_params_to_inactive(dup_curve_dict)

# assumes incoming duplicates are to update the old parameters.
    update_curves = (parameters_table[parameters_table[id_col]
                     .isin(dup_curves)])
    updatedParameters = new_record_metadata(update_curves)
    write_to_database(updatedParameters)

# for new curves- insert data
    new_curves = (parameters_table[parameters_table[id_col]
                  .isin(dup_curves) == False])

    fresh_curves = new_record_metadata(new_curves)
    write_to_database(fresh_curves)


current_user = os.getlogin()
operation_time = datetime.datetime.now()
result = update_parameters_table()

dataTable = reload_parameters_table()