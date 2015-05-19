# import msdb to python data object
# http://stackoverflow.com/questions/17910657/pyodbc-error-of-data-source-name-not-found-or-no-default-driver-on-64-bit-pc

import pypyodbc

def cursor_dtypes(cursor):
    import numpy as np
    import decimal
    import datetime

    dtypes = []
    column_info = cursor.description

    for column in column_info:
        if column[1] == unicode:
            if column[3] > 1000:
                dtypes.append((column[0], 'U20000'))
            else:
                dtypes.append((column[0], 'U%d' % column[3]))

        elif column[1] == str:
            dtypes.append((column[0], 'S%d' % column[3]))
        elif column[1] == decimal.Decimal:
            dtypes.append((column[0], 'dd%d' % column[3]))
        elif column[1] == float:
            dtypes.append((column[0], 'f4'))
        elif column[1] == datetime.datetime:
            dtypes.append((column[0], 'O4'))
        elif column[1] == int:
            dtypes.append((column[0], 'i4'))

    return dtypes

def typecast_data(data, dtypes):
    import numpy as np

    print 'tc1'
    data = np.array(data)

    print 'tc2'
    dtypes = np.array(dtypes)

    print 'tc3'
    data_y, data_x = data.shape

    for i in range(data_x):
        if 'U' in dtypes[i,1]:
            for j in range(data_y):
                try:
                    data[j,i] = data[j,i].encode('utf-8')
                except:
                    data[j,i] = ''
            dtypes[i,1] = dtypes[i,1].replace('U','S')
        elif 'dd' in dtypes[i,1]:
            for j in range(data_y):
                try:
                    data[j,i] = float(data[j,i])
                except:
                    data[j,i] = np.nan
            dtypes[i,1] = 'f4'

    print 'tc4'
    dtypes = list(map(tuple,dtypes))
    #data = list(map(tuple,data))
    #print 'len data '+str(len(data))

    print 'tc5'
    dtypes = np.dtype(dtypes)
    print data
    print dtypes
    #data.astype(dtypes)
    #data = np.array(data, dtype=dtypes)

    return data

def process_cursor(cursor, data_frame=False):
    '''Return numpy dtype array from pypyodbc cursor with SQL query'''
    import numpy as np
    import pandas as pd

    print 'pc1'
    dtypes = cursor_dtypes(cursor)

    print 'pc2'
    data = typecast_data(cursor.fetchall(), dtypes)

    print 'pc3'
    if data_frame:
        output = pd.DataFrame.from_records(data)
    else:
        output = data

    return output

if __name__ == '__main__':
    import pandas as pd
    import os
    import numpy as np
    import tables

    msdb_file = 'C:/Users/ryan/Desktop/hg37g.mdb'
    msdb_name = os.path.splitext(os.path.split(msdb_file)[1])[0]
    bin_file = os.path.splitext(msdb_file)[0]+'.h5'

    #msdb_tables = ('cruise','ctd','deployments','diag','dive','haulout',
    #              'haulout_orig','summary','tag_info','uplink')

    msdb_tables = ('ctd','deployments','dive','gps','haulout',
                  'haulout_orig','sms','summary','tag_info','uplink')

    # If pandas/pickly object doesn't exist, create
    try:
        #panel = pd.read_pickle(bin_file)
        #data_db = np.read(bin_file)
        h5file = tables.openFile(bin_file, mode='r', title=msdb_name)
        table = h5file.root.detector.readout

    except:
        # Connect to MS Access database & create cursor
        connection = pypyodbc.win_connect_mdb(msdb_file)
        cursor = connection.cursor()

        p = {}
        for table in msdb_tables:

            print table+'1'
            # Query for by table name
            cursor.execute('Select * from '+table+';')

            print table+'2'
            # Create pandas array for table
            data_frame = process_cursor(cursor, data_frame=True)

            # Add table dataframe to dictionary
            p[table] = data_frame

        connection.close()

        # Create Pandas panel data object
        print '3'
        #panel = pd.Panel(p)
        # Pickle/bin
        print '4'
        #panel.to_pickle(bin_file)
        #np.save(bin_file, p)

        h5file = tables.openFile(bin_file, mode='w', title=msdb_name)
        root = h5file.root
        h5file.createArray(root, msdb_name, p)

    h5file.close()
