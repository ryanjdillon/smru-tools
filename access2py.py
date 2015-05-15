# import msdb to python data object
# http://stackoverflow.com/questions/17910657/pyodbc-error-of-data-source-name-not-found-or-no-default-driver-on-64-bit-pc

import pypyodbc

connection = pypyodbc.win_connect_mdb('C:/Users/ryan/Desktop/hg37.mdb')
cursor = connection.cursor()

mdb_tables = ('cruise','ctd','deployments','diag','dive','haulout','haulout_orig','summary','tag_info','uplink')

def get_dtypes(cursor):
  '''Return numpy dtype array from pypyodbc cursor with SQL query'''
  import numpy as np
  column_names = [column[0] for column in cursor.description]
  column_dtypes = [column[1] for column in cursor.description]
  dtypes = np.dtypes = zip(column_names,column_dtype)
  return dtypes

# Grant permission to user Admin for MSysObjects.Name reading
#cursor.execute("GRANT SELECT ON MSysObjects TO Admin;")

# Get table names from MSysObjects.Name
#print cursor.execute("SELECT MSysObjects.Name AS table_name "+
#                     "FROM MSysObjects "+
#                     "WHERE (((Left([Name],1))<>'~') "+
#                     "AND ((Left([Name],4))<>'MSys') "+
#                     "AND ((MSysObjects.Type) In (1,4,6))) "+
#                     "order by MSysObjects.Name;")

for table in mdb_tables:
  # If pandas/pickly object doesn't exist, create/

  # Query for by table name
  cursor.execute('Select * from '+table+';')

  # Create pandas array for table

  # Pickle/bin
  while True:
      row = cursor.fetchone()
      if row is None:
          break
      print row[0]
connection.close()
