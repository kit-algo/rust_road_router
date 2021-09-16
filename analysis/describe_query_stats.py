#!/usr/bin/env python3

import numpy as np
import pandas as pd

import json
import sys

pd.set_option('display.float_format', lambda x: '%.3f' % x)

queries = pd.DataFrame.from_records(json.load(sys.stdin)['algo_runs'])

if len(sys.argv) > 1:
  keys = sys.argv[1:]
else:
  keys = ['running_time_ms']

for key in keys:
  print(queries[key].describe())
