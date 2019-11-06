import os
import pandas as pd
import sqlite3


def parse(fn, frequency='hourly'):
    '''
    Read EnergyPlus SQLite database into a DataFrame.

    Parameters
    ----------
    fn : path-like object
        EnergyPlus SQL output to read
    frequency : str, default 'hourly'
        Fetch data with this reporting frequency

    Returns
    -------
    DataFrame
    '''

    if not os.path.exists(fn):
        raise OSError('File not found: %s' % fn)

    frequency = frequency.lower().capitalize()
    connection = sqlite3.connect(fn)

    # Construct SQL query
    data_sql = """
        SELECT KeyValue, VariableName, VariableValue, SimulationDays, \
                Time.TimeIndex
        FROM ReportVariableData
        INNER JOIN ReportVariableDataDictionary
        ON ReportVariableDataDictionary.ReportVariableDataDictionaryIndex \
                = ReportVariableData.ReportVariableDataDictionaryIndex
        INNER JOIN Time
        ON Time.TimeIndex = ReportVariableData.TimeIndex
        WHERE ReportingFrequency == ?
    """
    raw_df = pd.read_sql(data_sql, connection, params=(frequency,))

    connection.close()

    return raw_df.pivot_table(values='VariableValue', index=['TimeIndex'],
                              columns=['VariableName', 'KeyValue'])


def _filter_column(col, search, dest, name='drop', verbose=False):
    for query in search:
        q = query.lower()
        if q in col[0].lower() or q in col[1].lower():
            if verbose:
                print(name, col)
            if dest:
                dest.add(col)
            return True
    return False


def get_uxy(df, targets=['Electricity:Facility', 'Gas:Facility'],
            verbose=False):
    '''
    Split dataset into u, x, y dataframes.

    Parameters
    ----------
    df : pandas.DataFrame
        the dataframe to split
    targets : list
        y variables to use (default Electricity:Facility and Gas:Facility)
    verbose : bool
        print debug information (default False)

    Returns
    -------
    u : pandas.DataFrame
        dataframe with control and environmental columns
    x : pandas.DataFrame
        dataframe with state variables
    y : pandas.DataFrame
        dataframe with output variables
    '''

    u_cols, x_cols, y_cols = set(), set(), set()

    drop_search = ['plenum', 'control type', 'sensible load to',
                   'water heater:watersystems:gas', 'plant supply',
                   'plant system', 'people air temperature',
                   ':electricity', ':gas', 'cooling energy',
                   'heating energy', 'boiler', 'chiller', 'electric energy',
                   'fan energy', 'air system']
    u_search = ['setpoint temperature', 'people occupant count',
                'outdoor', 'schedule value']
    x_search = ['zone mean air temperature', 'zone air temperature',
                'zone air humidity ratio']
    y_search = targets

    for c in df.columns:
        # c[0] contains name of attribute
        # c[1] contains zone or system to which it applies

        # Remove plenum zones because they have no control inputs
        if _filter_column(c, drop_search, None, verbose=verbose):
            continue
        if _filter_column(c, u_search, u_cols, name='u', verbose=verbose):
            continue
        if _filter_column(c, x_search, x_cols, name='x', verbose=verbose):
            continue
        if _filter_column(c, y_search, y_cols, name='y', verbose=verbose):
            continue

        if verbose:
            print('undecided:', c)

    return df[list(u_cols)], df[list(x_cols)], df[list(y_cols)]


def get_zones(fn):
    '''
    Read zones information from EnergyPlus SQLite database into a DataFrame.

    Parameters
    ----------
    fn : path-like object
        EnergyPlus SQL output to read
    frequency : str, default 'hourly'
        Fetch data with this reporting frequency

    Returns
    -------
    DataFrame
    '''

    if not os.path.exists(fn):
        raise OSError('File not found: %s' % fn)

    connection = sqlite3.connect(fn)

    # Construct SQL query
    data_sql = """
        SELECT *
        FROM Zones
    """
    raw_df = pd.read_sql(data_sql, connection)
    raw_df.set_index('ZoneIndex', inplace=True)

    connection.close()

    return raw_df
