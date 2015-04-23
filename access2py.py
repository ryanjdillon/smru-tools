# import msdb to python data object
# http://stackoverflow.com/questions/17910657/pyodbc-error-of-data-source-name-not-found-or-no-default-driver-on-64-bit-pc

import pypyodbc

connection = pypyodbc.win_connect_mdb('C:/Users/ryan/Desktop/hg37.mdb')
cursor = connection.cursor()

mdb_tables = ('cruise','ctd','deployments','diag','dive','haulout','haulout_orig','summary','tag_info','uplink')

cruise_dtypes = np.dtype([('ref',str),        #00 - Simulation day number
                          ('PTT',int),       #01 - Whale Id
                          ('S_DATE',),
                          ('E_DATE',),
                          ('CRUISE_NUMBER',),
                          ('CNT',),
                          ('LAT',float),         #02 - Longitude
                          ('LON',),
                          ('END_LAT',),
                          ('END_LON',),
                          ('UPLINK',)
                          ])


ctd_dtypes = np.dtype([('ref',),
                     ('PTT',),
                     ('END_DATE',),
                     ('MAX_DBAR',),
                     ('LAT',),
                     ('LON',),
                     ('NUM',),
                     ('N_TEMP',),
                     ('N_COND',),
                     ('N_SAL',),
                     ('TEMP_DBAR',),
                     ('TEMP_VALS',),
                     ('COND_DBAR',),
                     ('COND_VALS',),
                     ('SAL_DBAR',),
                     ('SAL_VALS',),
                     ('N_FLUORO',),
                     ('FLUORO_DBAR',),
                     ('FLUORO_VALS',),
                     ('N_OXY',),
                     ('OXY_DBAR',),
                     ('OXY_VALS',),
                     ('QC_PROFILE',),
                     ('QC_TEMP',),
                     ('QC_SAL',),
                     ('SAL_CORRECTED_VALS',)
                     ])

deployments_dtypes=np.dtype([('ref',),
                           ('PTT',),
                           ('ON_DATE',),
                           ('OFF_DATE',),
                           ('SPECIES',),
                           ('LOCATION',),
                           ('HOME_LAT',),
                           ('HOME_LON',),
                           ('VMASK_THRESHOLD',),
                           ('PROG',),
                           ('PARAMS',),
                           ('BODY',),
                           ('COMMENTS',),
                           ('N_PTS',),
                           ('TEST_DATE',),
                           ('T_OFFSET',),
                           ('T_SCALE',),
                           ('GREF',),
                           ('NAME',),
                           ('YEAR',),
                           ('RECOVERED',),
                           ('WMO',)
                           ])

diag_dtypes = np.dtype([('ref',),
                      ('PTT',),
                      ('D_DATE',),
                      ('LQ',),
                      ('LAT',),
                      ('LON',),
                      ('ALT_LAT',),
                      ('ALT_LON',),
                      ('N_MESS',),
                      ('N_MESS_120',),
                      ('BEST_LEVEL',),
                      ('PASS_DUR',),
                      ('FREQ',),
                      ('V_MASK',),
                      ('ALT',),
                      ('EST_SPEED',),
                      ('KM_FROM_HOME',),
                      ('IQ',),
                      ('NOPS',),
                      ('DELETED',),
                      ('ACTUAL_PTT',)
                      ])

dive_dtypes = np.dtype([('ref',),
                      ('PTT',),
                      ('CNT',),
                      ('DE_DATE',),
                      ('SURF_DUR',),
                      ('DIVE_DUR',),
                      ('MAX_DEP',),
                      ('D1',),
                      ('D2',),
                      ('D3',),
                      ('D4',),
                      ('V1',),
                      ('V2',),
                      ('V3',),
                      ('V4',),
                      ('V5',),
                      ('TRAVEL_R',),
                      ('HOMEDIST',),
                      ('BOTTOM',),
                      ('T1',),
                      ('T2',),
                      ('T3',),
                      ('N_SPEEDS',),
                      ('DEPTH_ST',),
                      ('SPEED_STR',),
                      ('PROPN_S',),
                      ('D5',),
                      ('T5',),
                      ('qc',),
                      ('D6',),
                      ('D7',),
                      ('D8',),
                      ('D9',),
                      ('T6',),
                      ('T7',),
                      ('T8',),
                      ('T9',),
                      ('ds_date',),
                      ('start_lat',),
                      ('start_lon',),
                      ('lat',),
                      ('lon',)
                      ])

haulout =np.dtype([('ref',),
                   ('PTT',),
                   ('S_DATE',),
                   ('E_DATE',),
                   ('HAULOUT_NUMBER',),
                   ('CNT',),
                   ('end_number',),
                   ('lat',),
                   ('lon',)
                   ])

haulout_orig =np.dtype([('ref',),
                        ('PTT',),
                        ('S_DATE',),
                        ('E_DATE',),
                        ('HAULOUT_NUMBER',),
                        ('CNT',),
                        ('lat',),
                        ('lon',)
                        ])

summary_dtypes =np.dtype([('ref',),
                        ('PTT',),
                        ('CNT',),
                        ('S_DATE',),
                        ('E_DATE',),
                        ('DIV_DIST',),
                        ('SURF_TM',),
                        ('DIVE_TM',),
                        ('HAUL_TM',),
                        ('N_CYCLES',),
                        ('AV_DEPTH',),
                        ('MAX_DEPTH',),
                        ('CRUISE_TM',),
                        ('AVG_SST',),
                        ('AVG_SPEED',),
                        ('SD_DEPTH',),
                        ('AV_DUR',),
                        ('SD_DUR',),
                        ('MAX_DUR',),
                        ('DP_N_CYCLES',),
                        ('DP_AV_DEPTH',),
                        ('DP_MAX_DEPTH',),
                        ('DP_AVG_SPEED',),
                        ('DP_SD_DEPTH',),
                        ('DP_AV_DUR',),
                        ('DP_SD_DUR',),
                        ('DP_MAX_DUR',),
                        ('DP_DIVE_TM',),
                        ('AV_SURF_DUR',),
                        ('SD_SURF_DUR',),
                        ('MAX_SURF_DUR',),
                        ('DP_AV_SURF_DUR',),
                        ('DP_SD_SURF_DUR',),
                        ('DP_MAX_SURF_DUR',)
                        ])

tag_info_dtypes = np.dtype([('ref',),
                          ('PTT',),
                          ('U_DATE',),
                          ('TX_NUMBER',),
                          ('OCLOCK',),
                          ('TAG_TIME',),
                          ('MAX_DEPTH',),
                          ('ABORTED_TX',),
                          ('BODY',),
                          ('LATEST_RESET',),
                          ('N_RESETS',),
                          ('ADC_OFFSET',),
                          ('WD_STATUS',),
                          ('N_WD_FAIL',),
                          ('N_ODO_FAIL',),
                          ('DRY_COND',),
                          ('WET_COND',),
                          ('GPS_NONE',),
                          ('GPS_SUB5',),
                          ('GPS_GOOD',),
                          ('GPS_REBOOT',),
                          ('TEMPERATURE',),
                          ('ODOMETER',),
                          ('BATTERY_MV',),
                          ('HOURLY_TX_ALLOWANC',),
                          ('DEPTH_SPIKES',),
                          ('CTD_SAMPLES',)
                          ])

uplink_dtypes = np.dtype([('ref',),
                        ('PTT',),
                        ('U_DATE',),
                        ('PASS',),
                        ('SAT',),
                        ('HEX',),
                        ('CI',),
                        ('',), #TODO check what this is about
                        ('SOURCE',),
                        ('CHECKSUM',),
                        ('DECODED',),
                        ('ACTUAL_PTT',)
                        ])

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
  cursor.execute('Select * from '+table+';')

  while True:
      row = cursor.fetchone()
      if row is None:
          break
      print row[0]
connection.close()
