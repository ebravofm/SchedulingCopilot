import jpype
jpype.startJVM()
import pandas as pd
import json
import numpy as np
from utils import read_json, dfs_to_inputs, split_tasks_df, gen_num_dates, print_excel
from solver import solve
from msproject import generate_mspdi

# Load input data
tasks_df, squads_df, tools_df = read_json('tasks.json')

# Split tasks into subsets
dfs = split_tasks_df(tasks_df)
print(f'Tasks split into {len(dfs)} subsets')

# Scaling factor
scaling = 4

# Initialize results dictionary
task_results = {}

# Solve each subset
for i, df in enumerate(dfs):
    print(f'Solving subset {i + 1}/{len(dfs)}')
    tasks, task_windows, task_groups, max_impact, resource_capacities, resources_forbidden_intervals, min_date = dfs_to_inputs(df, squads_df, tools_df, scaling)
    result = solve(tasks, task_windows, task_groups, max_impact, resource_capacities, resources_forbidden_intervals, min_date, scaling)
    task_results = task_results | result

print('Solving complete')

# Save results to a JSON file
output_file = "solution.json"
with open(output_file, "w") as json_file:
    json.dump(task_results, json_file, indent=4)
print(f'Results saved to {output_file}')

# Ask user which outputs to generate
generate_mspdi_output = input("Generate Microsoft Project file? (yes/no): ").strip().lower() == "yes"
generate_excel_output = input("Generate Excel file? (yes/no): ").strip().lower() == "yes"

if generate_mspdi_output:
    generate_mspdi('tasks.json', 'solution.json', 'solution.xml')
    print('Microsoft Project file generated as solution.xml')

if generate_excel_output:
    print_excel('tasks.json', 'solution.json', 'solution.xlsx')
    print('Excel file generated as solution.xlsx')
