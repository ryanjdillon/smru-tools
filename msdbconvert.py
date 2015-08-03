# import msdb to python data object
# http://stackoverflow.com/questions/17910657/pyodbc-error-of-data-source-name-not-found-or-no-default-driver-on-64-bit-pc

import os
import numpy as np
import pypyodbc
import h5py

def cursor_dtypes(cursor):
    import numpy as np
    import decimal
    import datetime

    dtypes = []
    dtypes_str = []
    column_info = cursor.description

    for column in column_info:

        print 'column0',column[0]

        if column[1] == unicode:
            if column[3] > 1000:
                dtypes.append((column[0], 'S250'))
                dtypes_str.append('list')
                print column[0],column[3],column[1]
            else:
                dtypes.append((column[0], 'S%d' % column[3]))
                dtypes_str.append('str')
        elif column[1] == str:
            dtypes.append((column[0], 'S%d' % column[3]))
            dtypes_str.append('str')
        elif column[1] == decimal.Decimal:
            #dtypes.append((column[0], 'dd%d' % column[3]))
            dtypes.append((column[0], 'f%d' % column[3]))
            dtypes_str.append('dd')
        elif column[1] == float:
            dtypes.append((column[0], 'f4'))
            dtypes_str.append('f')
        elif column[1] == datetime.datetime:
            dtypes.append((column[0], 'S10'))
            dtypes_str.append('datetime')
        elif column[1] == int:
            dtypes.append((column[0], 'i4'))
            dtypes_str.append('i')

    dtypes = np.dtype(dtypes)

    return dtypes, dtypes_str


def process_cursor(cursor, h5file, table):
    '''Return numpy dtype array from pypyodbc cursor with SQL query'''

    # Query for by table name
    cursor.execute('Select * from '+table+';')

    print 'pc1'
    dtypes, dtypes_str = cursor_dtypes(cursor)

    print 'pc2'
    group_h5 = h5file.create_group(table)
    print group_h5

    # Get number of rows
    num_rows = len(cursor.fetchall())

    for i in range(len(dtypes)):
        param = h5file.create_dataset(table+'/'+ dtypes.names[i],
                                      (num_rows,),
                                      dtype = dtypes[i],
                                      maxshape = (None,),
                                      fillvalue = None
                                     )
    print 'pc3'

    # Query by table name
    cursor.execute('Select * from '+table+';')

    # Typecast and save data
    for i in range(num_rows):
        data = cursor.fetchone()
        for j in range(len(dtypes)):

            key_h5 = table + '/' + dtypes.names[j]

            if data != None:
                if ('S' in dtypes.descr[j][1]) or \
                   ('U' in dtypes.descr[j][1]):
                    if data[j] == None:
                        data_str = ''
                    elif dtypes_str[j] == 'datetime':
                        data_str = data[j].strftime('%Y-%m-%d')
                    else:
                        data_str = data[j].encode('utf-8')

                    h5file[key_h5][i] = data_str

                elif 'f' in dtypes.descr[j][1]:
                    try:
                        h5file[key_h5][i] = float(data[j])
                    except:
                        break
                        #h5file[key_h5][i] = -9999
                else:
                    h5file[key_h5][i] = data[j]

if __name__ == '__main__':

    #msdb_file = 'C:/Users/ryan/Desktop/hg37g.mdb'
    msdb_file = 'C:/Users/ryan/Desktop/hg37.mdb'
    msdb_name = os.path.splitext(os.path.split(msdb_file)[1])[0]
    bin_file = 'E:/access2py/'+msdb_name+'.h5'

    msdb_tables = ('cruise','ctd','deployments','diag','dive','haulout',
                  'haulout_orig','summary','tag_info','uplink')

    #msdb_tables = ('ctd','deployments','dive','gps','haulout',
    #              'haulout_orig','sms','summary','tag_info','uplink')

    # If pandas/pickly object doesn't exist, create
    #if not os.path.isfile(bin_file):

    # Connect to MS Access database & create cursor
    connection = pypyodbc.win_connect_mdb(msdb_file)
    cursor = connection.cursor()

    #h5file = tables.openFile(bin_file, mode='w', title=msdb_name)

    h5file = h5py.File(bin_file, 'w')

    for table in msdb_tables:

        print table+'1'

        # Append table to HDF5 File
        process_cursor(cursor, h5file, table)

    h5file.close()
    connection.close()

    #else:
    #    print bin_file+' already created.'
