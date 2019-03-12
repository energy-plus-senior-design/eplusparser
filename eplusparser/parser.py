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
                              columns=['KeyValue', 'VariableName'])
