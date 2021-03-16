#!/usr/bin/env python3

import numpy as np
import pandas as pd

import json
import sys

baseline = json.load(open(sys.argv[1]))
baseline_queries = pd.DataFrame.from_records(baseline['algo_runs'])

# baseline_queries[key] = baseline_queries[key] - baseline_queries['baseline_pot_init_running_time_ms']

compare = json.load(open(sys.argv[2]))
compare_queries = pd.DataFrame.from_records(compare['algo_runs'])

if len(sys.argv) > 3:
  key = sys.argv[3]
else:
  key = 'running_time_ms'

print("Baseline: {}".format(baseline['git_revision']))
print(baseline_queries[key].describe())

print("Compare: {}".format(compare['git_revision']))
print(compare_queries[key].describe())

diff = compare_queries[key] - baseline_queries[key]
print("Difference: ")
print(diff.describe())

rel_diff = compare_queries[key] / baseline_queries[key]
print("Compare / Baseline: ")
print(rel_diff.describe())
