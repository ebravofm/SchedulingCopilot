import pandas as pd
import numpy as np
from ortools.sat.python import cp_model
from typing import Dict, Any
from app.schemas import InputData, OutputData

def schedule_tasks(input_data: InputData) -> Dict[str, Any]:
    # Convertir las tareas y escuadrones en DataFrames
    df = pd.DataFrame([task.dict() for task in input_data.tasks])
    squads_df = pd.DataFrame([squad.dict() for squad in input_data.squads])

    scaling = 4
    hours = 24
    df['Len'] = np.round(df['Len'] * scaling).astype(int)
    df['Len'] = np.where(df['Len'] == 0, 1, df['Len'])
    df['EarliestDate'] = df['EarliestDate'] * scaling
    df['RequiredDate'] = df['RequiredDate'] * scaling

    # Asegurar que 'Task' sea el índice
    if 'Task' in df.columns:
        df.set_index('Task', inplace=True)
    else:
        df['Task'] = df.index

    # Construir task_windows y tasks
    task_windows = {idx: (row['EarliestDate'], row['RequiredDate']) for idx, row in df.iterrows()}
    tasks = {}
    for idx, row in df.iterrows():
        duration = row['Len']
        required_resources = []
        if pd.notnull(row.get('Squad', None)):
            required_resources.append(int(row['Squad']))
        if pd.notnull(row.get('Tool', None)):
            required_resources.append(int(row['Tool']))
        cantidad = row.get('Q', 1)
        impact = row.get('Impact', 0)
        tasks[idx] = (duration, required_resources, cantidad, impact)

    max_makespan = df['RequiredDate'].max()
    max_impact = df['Impact'].max()

    # Construir diccionarios de herramientas y escuadrones
    tools_list = df['Tool'].dropna().unique()
    tools = {t: [1, 1] for t in tools_list}
    squads = {idx: [row['Cap'], 0] for idx, row in squads_df.iterrows()}
    resource_capacities = {**squads, **tools}

    # Construir grupos de tareas si existen
    if 'TaskGroup' in df.columns:
        task_groups = df.groupby('TaskGroup')
        task_groups = {idx: t.sort_values('Task').index.tolist() for idx, t in task_groups if pd.notnull(idx)}
    else:
        task_groups = {}

    # Definir funciones auxiliares
    def generar_intervalos(row, active_column, inactive_column, max_time=300):
        start = row.get('Start', 0)
        active = row[active_column]
        inactive = row[inactive_column]
        intervalos_activos = []
        intervalos_inactivos = []
        tiempo_actual = start
        if tiempo_actual > 0:
            intervalos_inactivos.append((0, tiempo_actual))
        while tiempo_actual < max_time:
            fin_activo = min(tiempo_actual + active, max_time)
            intervalos_activos.append((tiempo_actual, fin_activo))
            tiempo_actual = fin_activo
            if tiempo_actual >= max_time:
                break
            fin_inactivo = min(tiempo_actual + inactive, max_time)
            intervalos_inactivos.append((tiempo_actual, fin_inactivo))
            tiempo_actual = fin_inactivo
        return intervalos_activos, intervalos_inactivos

    def combine(intervalos_dict1, intervalos_dict2):
        intervalos_combinados = {}
        for resource in set(intervalos_dict1.keys()).union(intervalos_dict2.keys()):
            intervalos1 = intervalos_dict1.get(resource, [])
            intervalos2 = intervalos_dict2.get(resource, [])
            todos_intervalos = intervalos1 + intervalos2
            todos_intervalos.sort(key=lambda x: x[0])
            intervalos_simplificados = []
            for intervalo in todos_intervalos:
                if not intervalos_simplificados:
                    intervalos_simplificados.append(intervalo)
                else:
                    ultimo_intervalo = intervalos_simplificados[-1]
                    if intervalo[0] <= ultimo_intervalo[1]:
                        intervalos_simplificados[-1] = (ultimo_intervalo[0], max(ultimo_intervalo[1], intervalo[1]))
                    else:
                        intervalos_simplificados.append(intervalo)
            intervalos_combinados[resource] = intervalos_simplificados
        return intervalos_combinados

    # Aplicar 'generar_intervalos' a squads_df
    squads_df['ActiveDays'] = squads_df['ActiveDays'] * hours
    squads_df['InactiveDays'] = squads_df['InactiveDays'] * hours
    squads_df['ActiveHoursIntervals'], squads_df['InactiveHoursIntervals'] = zip(*squads_df.apply(
        generar_intervalos, axis=1, args=('ActiveHours', 'InactiveHours', max_makespan,)))
    squads_df['ActiveDaysIntervals'], squads_df['InactiveDaysIntervals'] = zip(*squads_df.apply(
        generar_intervalos, axis=1, args=('ActiveDays', 'InactiveDays', max_makespan,)))

    squad_forbidden_hours_intervals = {idx: row['InactiveHoursIntervals'] for idx, row in squads_df.iterrows()}
    squad_forbidden_days_intervals = {idx: row['InactiveDaysIntervals'] for idx, row in squads_df.iterrows()}
    squad_forbidden_intervals = combine(squad_forbidden_hours_intervals, squad_forbidden_days_intervals)
    tools_forbidden_intervals = {t: [] for t in df['Tool'].dropna().unique()}
    resources_forbidden_intervals = {**squad_forbidden_intervals, **tools_forbidden_intervals}

    # Definir el modelo de optimización
    model = cp_model.CpModel()
    is_scheduled = {}
    task_starts = {}
    task_ends = {}
    task_intervals = {}

    # Añadir tareas y restricciones
    for task_id, (duration, required_resources, cantidad, impact) in tasks.items():
        is_scheduled[task_id] = model.NewBoolVar(f'is_scheduled_{task_id}')
        start_var = model.NewIntVar(task_windows[task_id][0], task_windows[task_id][1] - duration, f'start_{task_id}')
        end_var = model.NewIntVar(task_windows[task_id][0] + duration, task_windows[task_id][1], f'end_{task_id}')
        interval_var = model.NewOptionalIntervalVar(
            start_var, duration, end_var, is_scheduled[task_id], f'interval_{task_id}')
        task_starts[task_id] = start_var
        task_ends[task_id] = end_var
        task_intervals[task_id] = interval_var

        # Evitar intervalos prohibidos
        for resource_id in required_resources:
            for forbidden_start, forbidden_end in resources_forbidden_intervals.get(resource_id, []):
                before_interval = model.NewBoolVar(f'before_{task_id}_{forbidden_start}_{forbidden_end}')
                after_interval = model.NewBoolVar(f'after_{task_id}_{forbidden_start}_{forbidden_end}')
                model.Add(end_var <= forbidden_start).OnlyEnforceIf(before_interval)
                model.Add(start_var >= forbidden_end).OnlyEnforceIf(after_interval)
                model.AddBoolOr([before_interval, after_interval, is_scheduled[task_id].Not()])
                model.Add(before_interval == 0).OnlyEnforceIf(is_scheduled[task_id].Not())
                model.Add(after_interval == 0).OnlyEnforceIf(is_scheduled[task_id].Not())

    # Añadir restricciones cumulativas para cada recurso
    for resource_id, (capacity, type_) in resource_capacities.items():
        intervals = []
        demands = []
        for task_id, (duration, required_resources, cantidad, impact) in tasks.items():
            if resource_id in required_resources:
                intervals.append(task_intervals[task_id])
                if type_ == 0:
                    demands.append(cantidad)
                else:
                    demands.append(1)
        model.AddCumulative(intervals, demands, capacity)

    # Restricciones de grupos de tareas
    group_scheduled = {}
    for group_id, group in task_groups.items():
        group_var = model.NewBoolVar(f'group_scheduled_{group_id}')
        group_scheduled[group_id] = group_var
        for task_id in group:
            model.Add(is_scheduled[task_id] == group_var)
        previous_task_id = None
        for task_id in group:
            if previous_task_id is not None:
                model.Add(task_starts[task_id] >= task_starts[previous_task_id] + tasks[previous_task_id][0]).OnlyEnforceIf(group_var)
            previous_task_id = task_id

    # Calcular pesos basados en 'Impact'
    weights = {task_id: (max_impact + 1 - tasks[task_id][3]) ** 3 for task_id in tasks}
    model.Minimize(sum(weights[task_id] * (1 - is_scheduled[task_id]) for task_id in tasks))

    solver = cp_model.CpSolver()
    solver.parameters.log_search_progress = True
    solver.parameters.max_time_in_seconds = 10
    solver.parameters.relative_gap_limit = 0.25
    status = solver.Solve(model)

    output = []
    if status == cp_model.OPTIMAL or status == cp_model.FEASIBLE:
        for task_id in tasks:
            scheduled = solver.Value(is_scheduled[task_id])
            if scheduled:
                start = solver.Value(task_starts[task_id])
                end = solver.Value(task_ends[task_id])
            else:
                start = None
                end = None
            output.append({
                'Task': task_id,
                'Scheduled': bool(scheduled),
                'Start': start,
                'End': end
            })
    else:
        for task_id in tasks:
            output.append({
                'Task': task_id,
                'Scheduled': False,
                'Start': None,
                'End': None
            })

    return {'tasks': output}
