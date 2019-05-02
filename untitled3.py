# -*- coding: utf-8 -*-
"""
Created on Wed May  1 14:59:45 2019

@author: lfontenelle
"""


import datetime


class WellSlugging:

	def transform_load(df, coll):
		#print('Started  Transform')
		result_df = {}
		result_df['Alert'] = 'WellSlugging'
		result_df['Source'] = 'CYGNET'

		print('WellSlugging.py:  '+ str(df.shape))
		#print(str(df.head(3)))
		apis = df.api.unique()

		for api in apis:
			df_api = df.loc[df['api'] == api]
			df_FIG = df_api.loc[df_api['udc'] == 'FI_G']
			df_PIC = df_api.loc[df_api['udc'] == 'PI_C']

			mean_FI_G = df_FIG['value'].mean()
			min_FI_G = df_FIG['value'].min()
			max_FI_G = df_FIG['value'].max()

			mean_PI_C = df_PIC['value'].mean()
			min_PI_C = df_PIC['value'].min()
			max_PI_C = df_PIC['value'].max()

			# print("mean_FI_G: "+str(mean_FI_G)+ "    -max_FI_G:"+str(max_FI_G)+"    -min_FI_G:"+str(min_FI_G))
			# print("mean_PI_C: "+str(mean_PI_C)+ "    -max_PI_C:"+str(max_PI_C)+"    -min_PI_C:"+str(min_PI_C))

			if(min_FI_G <= 0):
				trigger = False
			elif (((max_FI_G - min_FI_G) >= 75) and (max_PI_C - min_PI_C >= 25)):
				trigger = True
			else:
				trigger = False

			result_df['min_FI_G'] = min_FI_G
			result_df['mean_FI_G'] = mean_FI_G
			result_df['trigger'] = trigger
			result_df['time_alert'] = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
			result_df['api'] = api

			_id = str(datetime.datetime.now())+str(api)
			coll.update({'_id':_id}, result_df, True)

