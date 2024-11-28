import pandas as pd
import json
import numpy as np
import networkx as nx
from datetime import timedelta

pd.options.mode.chained_assignment = None

def read_json(json_file):
    with open(json_file, "r") as f:
        data = json.load(f)
    squads_df = pd.DataFrame(data["Squads"])
    tools_df = pd.DataFrame(data["Tools"])
    tasks_df = pd.DataFrame(data["Tasks"])
    
    tasks_df['EarliestDate'] = pd.to_datetime(tasks_df['EarliestDate'])
    tasks_df['RequiredDate'] = pd.to_datetime(tasks_df['RequiredDate'])
    
    tasks_df['RequiredDate'] = np.where(tasks_df['RequiredDate'] < tasks_df['EarliestDate'], tasks_df['EarliestDate'] + timedelta(days=7), tasks_df['RequiredDate'])
    
    return tasks_df, squads_df, tools_df


def dfs_to_inputs(tasks_df, squads_df, tools_df, scaling = 4):
    
    squads_df = squads_df[squads_df['SquadID'].isin(tasks_df['SquadID'])]
    tools_df = tools_df[tools_df['ToolID'].isin(tasks_df['ToolID'])]
    
    tasks_df['EarliestDateNum'], tasks_df['RequiredDateNum'], min_date = gen_num_dates(tasks_df)
    tasks_df = scale(tasks_df, scaling)
    tasks, task_windows, task_groups, max_impact, tools, squads, resource_capacities = get_inputs(tasks_df, squads_df, scaling)
    squads_df = get_shift_details(squads_df, scaling)
    resources_forbidden_intervals = get_forbidden_intervals(squads_df, tasks_df, tools)
        
    return tasks, task_windows, task_groups, max_impact, resource_capacities, resources_forbidden_intervals, min_date


def gen_num_dates(tasks_df, start_column_name='EarliestDate', finish_column_name='RequiredDate'):
    
    min_date = tasks_df[start_column_name].min()
    fecha_fin = tasks_df[finish_column_name].max()

    EarliestDateNum = (((tasks_df[start_column_name] - min_date).dt.total_seconds() / 86400) * 24).astype(int)
    RequiredDateNum = (((tasks_df[finish_column_name] - min_date).dt.total_seconds() / 86400) * 24).astype(int)
    EarliestDateNum = EarliestDateNum.apply(lambda x: 0 if x < 0 else x)
    RequiredDateNum = np.where(RequiredDateNum < EarliestDateNum +  + tasks_df.Duration, EarliestDateNum + tasks_df.Duration*10, RequiredDateNum)
    RequiredDateNum = RequiredDateNum.astype(int)
    RequiredDateNum = np.where(RequiredDateNum > 2000, 2000, RequiredDateNum)
    return EarliestDateNum, RequiredDateNum, min_date

def scale(tasks_df, scaling=4):
    
    tasks_df['Duration'] = np.round(tasks_df['Duration']*scaling).astype(int)
    tasks_df['Duration'] = np.where(tasks_df['Duration'] == 0, 1, tasks_df['Duration'])
    tasks_df['EarliestDateNum'] = tasks_df['EarliestDateNum']*scaling
    tasks_df['RequiredDateNum'] = tasks_df['RequiredDateNum']*scaling
    
    return tasks_df

def get_inputs(tasks_df, squads_df, scaling = 4):
    
    
    tasks = {idx: (row['Duration'], [int(val) for val in [row['SquadID'], row['ToolID']] if pd.notnull(val)], row['Workers'], row['Impact']) for idx, row in tasks_df.iterrows()}
    
    task_windows = {idx: (row['EarliestDateNum'], row['RequiredDateNum']) for idx, row in tasks_df.iterrows()}
    
    task_groups = tasks_df.groupby('OT')
    task_groups = {idx: t.sort_values('TaskID')['TaskID'].tolist() for idx, t in task_groups if len(t) > 1}
    
    max_impact = tasks_df['Impact'].max()
    tools = {int(t): [1, 1] for t in tasks_df['ToolID'].dropna().unique()}
    squads = {idx: [row['Capacity'], 0] for idx, row in squads_df.iterrows()}
    resource_capacities = squads | tools
    
    return tasks, task_windows, task_groups, max_impact, tools, squads, resource_capacities


def get_shift_details(squads_df, scaling):

    def decode_shift(shift):
        shift = shift.upper()
        
        A = shift[:3]
        B = shift[-1]
        
        if A == '4X4':
            ActiveHours = 12
            InactiveHours = 12
            ActiveDays = 4
            InactiveDays = 4
            if B == 'N':
                ShiftStart = 0
            else:
                ShiftStart = 12
        elif A == '5X2':
            ActiveHours = 8
            InactiveHours = 16
            ActiveDays = 5
            InactiveDays = 2
            if B == 'A':
                ShiftStart = 0
            elif B == 'B':
                ShiftStart = 8
            else:
                ShiftStart = 16
        elif A == '7X7':
            ActiveHours = 12
            InactiveHours = 12
            ActiveDays = 7
            InactiveDays = 7
            if B == 'N':
                ShiftStart = 0
            else:
                ShiftStart = 12
                
        return ActiveHours, InactiveHours, ActiveDays, InactiveDays, ShiftStart

    squads_df[['ActiveHours', 'InactiveHours', 'ActiveDays', 'InactiveDays', 'ShiftStart']] = \
        squads_df['Shift'].apply(decode_shift).apply(pd.Series)        
        
    squads_df['ActiveDays'] = squads_df['ActiveDays'] * 24 * scaling
    squads_df['InactiveDays'] = squads_df['InactiveDays'] * 24 * scaling
    squads_df['ActiveHours'] = squads_df['ActiveHours'] * scaling
    squads_df['InactiveHours'] = squads_df['InactiveHours'] * scaling
    squads_df['ShiftStart'] = squads_df['ShiftStart'] * scaling

    
    return squads_df
        
def get_forbidden_intervals(squads_df, tasks_df, tools, scaling=4, days=7):


    def generar_intervalos(row, active_column, inactive_column, max_time=672):  # max_time puede ser ajustado según el contexto
        start = row['ShiftStart']
        active = row[active_column]
        inactive = row[inactive_column]
        
        intervalos_activos = []
        intervalos_inactivos = []
        
        tiempo_actual = start
        
        # Si el primer intervalo activo no empieza en 0, agregar un intervalo inactivo al principio
        if tiempo_actual > 0:
            intervalos_inactivos.append((0, tiempo_actual))
        
        while tiempo_actual < max_time:
            # Intervalo activo
            fin_activo = min(tiempo_actual + active, max_time)
            intervalos_activos.append((tiempo_actual, fin_activo))
            
            # Avanzar al siguiente intervalo inactivo
            tiempo_actual = fin_activo
            if tiempo_actual >= max_time:
                break
            
            # Intervalo inactivo
            fin_inactivo = min(tiempo_actual + inactive, max_time)
            intervalos_inactivos.append((tiempo_actual, fin_inactivo))
            
            # Avanzar al siguiente intervalo activo
            tiempo_actual = fin_inactivo

        return intervalos_activos, intervalos_inactivos

    def combine(intervalos_dict1, intervalos_dict2):
        # Combinar los intervalos de ambos diccionarios
        intervalos_combinados = {}
        for squad in set(intervalos_dict1.keys()).union(intervalos_dict2.keys()):
            intervalos1 = intervalos_dict1.get(squad, [])
            intervalos2 = intervalos_dict2.get(squad, [])
            # Combinar y ordenar todos los intervalos
            todos_intervalos = intervalos1 + intervalos2
            todos_intervalos.sort(key=lambda x: x[0])  # Ordenar por tiempo de inicio
            
            # Simplificar los intervalos fusionando los que se solapan o son adyacentes
            intervalos_simplificados = []
            for intervalo in todos_intervalos:
                if not intervalos_simplificados:
                    intervalos_simplificados.append(intervalo)
                else:
                    ultimo_intervalo = intervalos_simplificados[-1]
                    # Si los intervalos se solapan o son adyacentes, fusionarlos
                    if intervalo[0] <= ultimo_intervalo[1]:
                        intervalos_simplificados[-1] = (ultimo_intervalo[0], max(ultimo_intervalo[1], intervalo[1]))
                    else:
                        intervalos_simplificados.append(intervalo)
            # Guardar los intervalos simplificados para el squad actual
            intervalos_combinados[squad] = intervalos_simplificados
            
        return intervalos_combinados

    # Aplicar la función a cada fila del DataFrame
    #squads_df['Intervalos Activos'], squads_df['Intervalos Inactivos'] = zip(*squads_df.apply(generar_intervalos, axis=1))
    max_makespan = 24 * days * scaling

    squads_df['ActiveHoursIntervals'], squads_df['InactiveHoursIntervals'] = zip(*squads_df.apply(generar_intervalos, axis=1, args=('ActiveHours', 'InactiveHours', max_makespan,)))
    squads_df['ActiveDaysIntervals'], squads_df['InactiveDaysIntervals'] = zip(*squads_df.apply(generar_intervalos, axis=1, args=('ActiveDays', 'InactiveDays', max_makespan,)))

    squad_forbidden_hours_intervals = {row['SquadID']: row['InactiveHoursIntervals'] for idx, row in squads_df.iterrows()}
    squad_forbidden_days_intervals = {row['SquadID']: row['InactiveDaysIntervals'] for idx, row in squads_df.iterrows()}
    #tools_forbidden_intervals = {t: [] for t in df['Tool'].dropna().unique()}
    squad_forbidden_intervals = combine(squad_forbidden_hours_intervals, squad_forbidden_days_intervals)

    tools_forbidden_intervals = {t: [] for t in tools.keys()}

    resources_forbidden_intervals = squad_forbidden_intervals | tools_forbidden_intervals
    
    return resources_forbidden_intervals

def split_tasks_df(tasks_df):
    relevant_df = tasks_df[['SquadID', 'ToolID']].dropna().drop_duplicates()
    graph = nx.Graph()
    
    for _, row in relevant_df.iterrows():
        graph.add_edge(f"Squad_{row['SquadID']}", f"Tool_{row['ToolID']}")

    connected_components = list(nx.connected_components(graph))
    subsets = []
    related_indices = set()

    for component in connected_components:
        squads = [float(node.split("_")[1]) for node in component if node.startswith("Squad_")]
        tools = [float(node.split("_")[1]) for node in component if node.startswith("Tool_")]
        subset = tasks_df[(tasks_df['SquadID'].isin(squads)) | (tasks_df['ToolID'].isin(tools))]
        subsets.append(subset)
        related_indices.update(subset.index)

    unrelated_tasks = tasks_df.drop(related_indices)
    unrelated_subsets = [unrelated_tasks[unrelated_tasks['SquadID'] == squad] 
                         for squad in unrelated_tasks['SquadID'].unique()]

    return subsets + unrelated_subsets
