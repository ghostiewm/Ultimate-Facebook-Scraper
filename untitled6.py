# -*- coding: utf-8 -*-
"""
Created on Tue Apr 30 15:48:37 2019

@author: lfontenelle
"""

from influxdb import InfluxDBClient as influx
import pandas as pd

#def writeToInflux():
client = influx("Soghoueon-min3", "8086",
                             "leftblankintentionally",
                             "leftblackintentionally", "db3")

data = client.query("""Select * from GASLIFT_CYGNET where  time > now() - 80m  AND ( "udc" = 'FI_G'  AND "facility_type" = 'METERGL')  OR  ("udc" = 'PI_C' AND "facility_type" = 'WELL' ) limit 1000 """, epoch= 'ms')

df = list(client.get_points(measurement = 'GASLIFT_CYGNET'))



df_call = list(data.get_points(measurement = 'GASLIFT_CYGNET'))
df = pd.DataFrame(df_call)


