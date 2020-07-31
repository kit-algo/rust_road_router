#!/usr/bin/env python3

import numpy as np
import pandas as pd

import json
import sys

baseline = json.load(open(sys.argv[1]))
baseline_queries = pd.DataFrame.from_records(baseline['algo_runs'])

# baseline_queries['running_time_ms'] = baseline_queries['running_time_ms'] - baseline_queries['baseline_pot_init_running_time_ms']

compare = json.load(open(sys.argv[2]))
compare_queries = pd.DataFrame.from_records(compare['algo_runs'])

print("Baseline: {}".format(baseline['git_revision']))
print(baseline_queries['running_time_ms'].describe())

print("Compare: {}".format(compare['git_revision']))
print(compare_queries['running_time_ms'].describe())

diff = compare_queries['running_time_ms'] - baseline_queries['running_time_ms']
print("Difference: ")
print(diff.describe())

rel_diff = compare_queries['running_time_ms'] / baseline_queries['running_time_ms']
print("Compare / Baseline: ")
print(rel_diff.describe())
