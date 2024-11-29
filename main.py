import jpype
jpype.startJVM()
import pandas as pd
import json
import numpy as np
from utils import read_json, dfs_to_inputs, split_tasks_df, gen_num_dates
from solver import solve
from msproject import generate_mspdi




tasks_df, squads_df, tools_df = read_json('tasks.json')
dfs = split_tasks_df(tasks_df)
print(f'Tasks splitted into {len(dfs)} subsets')
scaling = 4

task_results = {}

for i, df in enumerate(dfs):
    print(f'Solving subset {i+1}/{len(dfs)}')
    tasks, task_windows, task_groups, max_impact, resource_capacities, resources_forbidden_intervals, min_date = dfs_to_inputs(df, squads_df, tools_df, scaling = 4)
    result = solve(tasks, task_windows, task_groups, max_impact, resource_capacities, resources_forbidden_intervals, min_date, scaling)
    task_results = task_results | result
    
print('Solving complete')
output_file = "solution.json"
with open(output_file, "w") as json_file:
    json.dump(task_results, json_file, indent=4)  # indent=4 para hacer el archivo más legible

print(f'Output saved to {output_file}')

generate_mspdi('tasks.json', 'solution.json', 'solution.xml')
print('Project file generated')
