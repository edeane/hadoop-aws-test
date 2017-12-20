# -*- coding: utf-8 -*-
"""
Created on Wed Dec 13 09:01:40 2017

@author: Edward.Deane
"""

import pandas as pd
import numpy as np



df = pd.read_csv('us-500-3.csv', sep='|', index_col='index')

df.to_csv('aws/customers-with-header-500.csv', sep='|', index=True, index_label='index', header=True)

df.to_csv('aws/customers-with-out-header-500.csv', sep='|', index=True, index_label='index', header=False)



