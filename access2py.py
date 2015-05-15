# import msdb to python data object
# http://stackoverflow.com/questions/17910657/pyodbc-error-of-data-source-name-not-found-or-no-default-driver-on-64-bit-pc

import pypyodbc

def pyodbc_pandas(cursor):
  '''Return numpy dtype array from pypyodbc cursor with SQL query'''
  import numpy as np
  import pandas as pd
  import datetime

  colinfo = cursor.description
  dtypes = []
  for col in colinfo:
      if col[1] == unicode:
          dtypes.append((col[0], 'U%d' % col[3]))
      elif col[1] == str:
          dtypes.append((col[0], 'S%d' % col[3]))
      elif col[1] == float:
          dtypes.append((col[0], 'f4'))
      elif col[1] == datetime.datetime:
          dtypes.append((col[0], 'O4'))
      elif col[1] == int:
          dtypes.append((col[0], 'i4'))

  #dtypes = np.dtype(zip(column_names,dtypes))
  dtypes = np.dtype(dtypes)
  data = np.array(cursor.fetchall(), dtype=dtypes)

  data_frame = pd.DataFrame.from_records(data)

  return data_frame


connection = pypyodbc.win_connect_mdb('C:/Users/ryan/Desktop/hg37.mdb')
cursor = connection.cursor()

mdb_tables = ('cruise','ctd','deployments','diag','dive','haulout','haulout_orig','summary','tag_info','uplink')

#for table in mdb_tables:
  # If pandas/pickly object doesn't exist, create/

  # Query for by table name
  #cursor.execute('Select * from '+table+';')
cursor.execute('Select * from cruise;')

data_frame = pyodbc_pandas(cursor)

  # Create pandas array for table

  # Pickle/bin

connection.close()
