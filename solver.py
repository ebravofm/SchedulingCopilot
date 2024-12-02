import logging
from datetime import datetime, timedelta
from ortools.sat.python import cp_model
from utils import read_json, dfs_to_inputs, split_tasks_df
import uuid
import pandas as pd

# Configuración básica del logger
logging.basicConfig(
    filename="solver.log",
    level=logging.INFO,
    format="%(message)s",
    filemode="a"  # Abrir en modo append
)


    
def log_solver_run(
    uuid4,
    tasks, 
    task_results, 
    status, 
    solve_time, 
    makespan, 
    objective_value,
    solve_time_full
):
    """
    Función para registrar los detalles de una corrida del solver en un archivo de log.
    """
    # Fecha y hora de la corrida
    run_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    # Número de tareas
    num_tasks = len(tasks)
    
    # Crear línea de log
    log_line = (
        f"{str(uuid4)[-5:]}: "
        f"{run_time}, "
        f"Status: {status}, "
        f"Tasks: {num_tasks}, "
        f"Makespan: {makespan} hours, "
        f"Objective Value: {objective_value}, "
        f"Solve Time: {solve_time:.2f} seconds"
        f"Solve Time: {solve_time_full:.2f} seconds"
    )
    
    # Escribir en el log
    logging.info(log_line)


def run_solver(tasks_json='tasks.json' , split_tasks=True):


    start_timestamp = datetime.now()

    tasks_df, squads_df, tools_df = read_json(tasks_json)
    uuid4 = uuid.uuid4()

    # Split tasks into subsets if split_tasks is True
    if split_tasks:
        dfs = split_tasks_df(tasks_df)
    else:
        dfs = [tasks_df]

    # Scaling factor
    scaling = 4

    # Initialize results dictionary
    task_results = {}

    # Solve each subset
    for i, df in enumerate(dfs):
        print(f'Solving subset {i + 1}/{len(dfs)}')
        tasks, task_windows, task_groups, max_impact, resource_capacities, resources_forbidden_intervals, min_date = dfs_to_inputs(df, squads_df, tools_df, scaling)
        result = solve(tasks, task_windows, task_groups, max_impact, resource_capacities, resources_forbidden_intervals, min_date, scaling, uuid4=uuid4)
        task_results = task_results | result
        
    solve_time = (datetime.now() - start_timestamp).total_seconds()
    metrics = calculate_metrics(tasks_json, task_results)
    
    #     metrics  {
    #     "Total Tasks": total_tasks,
    #     "Scheduled Tasks": scheduled_tasks,
    #     "Unscheduled Tasks": unscheduled_tasks,
    #     "Makespan": makespan,
    #     "Average Makespan": average_makespan,
    #     "Objective Value": objective_value
    # }

    
    logging.info(f"Total Tasks: {metrics['Total Tasks']}, Scheduled Tasks: {metrics['Scheduled Tasks']}, Unscheduled Tasks: {metrics['Unscheduled Tasks']}, Makespan: {metrics['Makespan']}, Objective Value: {metrics['Objective Value']}, Average OT Makespan: {metrics['Average Makespan']}, Solve Time: {solve_time:.2f} seconds\n")

    
    return task_results


def solve(tasks, task_windows, task_groups, max_impact, resource_capacities, resources_forbidden_intervals, min_date, scaling, uuid4):
    
    start_timestamp = datetime.now()

    # Definir el modelo de optimización
    model = cp_model.CpModel()
    is_scheduled = {}
    task_starts = {}
    task_ends = {}
    task_intervals = {}

    # Restricciones de Ventanas de Tiempo para las tareas
    for task_id, (duration, required_resources, cantidad, impact) in tasks.items():
        is_scheduled[task_id] = model.NewBoolVar(f'is_scheduled_{task_id}')
        start_var = model.NewIntVar(task_windows[task_id][0], task_windows[task_id][1] - duration, f'start_{task_id}')
        end_var = model.NewIntVar(task_windows[task_id][0] + duration, task_windows[task_id][1], f'end_{task_id}')
        interval_var = model.NewOptionalIntervalVar(start_var, duration, end_var, is_scheduled[task_id], f'interval_{task_id}')
        task_starts[task_id] = start_var
        task_ends[task_id] = end_var
        task_intervals[task_id] = interval_var
        
        
        # Añadir restricciones para evitar que las tareas se programen durante los intervalos prohibidos
        for resource_id in required_resources:
            for forbidden_start, forbidden_end in resources_forbidden_intervals[resource_id]:
                # Variables booleanas que indican si la tarea termina antes o comienza después del intervalo prohibido
                before_interval = model.NewBoolVar(f'before_{task_id}_{forbidden_start}_{forbidden_end}')
                after_interval = model.NewBoolVar(f'after_{task_id}_{forbidden_start}_{forbidden_end}')

                # Si 'before_interval' es verdadero, la tarea termina antes de que comience el intervalo prohibido
                model.Add(end_var <= forbidden_start).OnlyEnforceIf(before_interval)
                # Si 'after_interval' es verdadero, la tarea comienza después de que termina el intervalo prohibido
                model.Add(start_var >= forbidden_end).OnlyEnforceIf(after_interval)

                # La tarea debe estar programada fuera del intervalo prohibido si está programada
                model.AddBoolOr([before_interval, after_interval, is_scheduled[task_id].Not()])

                # Enlazar 'before_interval' y 'after_interval' con 'is_scheduled' para evitar programar tareas no planificadas
                model.Add(before_interval == 0).OnlyEnforceIf(is_scheduled[task_id].Not())
                model.Add(after_interval == 0).OnlyEnforceIf(is_scheduled[task_id].Not())
        


    # Añadir restricciones cumulativas para cada recurso
    for resource_id, (capacity, type) in resource_capacities.items():
        intervals = []
        demands = []
        for task_id, (duration, required_resources, cantidad, impact) in tasks.items():
            if resource_id in required_resources:
                intervals.append(task_intervals[task_id])
                if type == 0:
                    demands.append(cantidad)
                else:
                    demands.append(1)
        model.AddCumulative(intervals, demands, capacity)

    # Crear variables de programación para los grupos
    group_scheduled = {}
    for group_id, group in task_groups.items():
        group_var = model.NewBoolVar(f'group_scheduled_{group_id}')
        group_scheduled[group_id] = group_var
        for task_id in group:
            model.Add(is_scheduled[task_id] == group_var)

        # Agregar restricciones de precedencia entre tareas consecutivas dentro del grupo
        previous_task_id = None
        for task_id in group:
            if previous_task_id is not None:
                # Si el grupo está programado, asegurar la precedencia entre tareas
                model.Add(task_starts[task_id] >= task_starts[previous_task_id]+tasks[previous_task_id][0]).OnlyEnforceIf(group_var)
            previous_task_id = task_id


    # Calcular pesos inversos basados en 'Impact'
    weights = {task_id: (max_impact + 1 - tasks[task_id][3]) ** 3 for task_id in tasks}

    # Definir la función objetivo ponderada
    model.Minimize(sum(weights[task_id] * (1 - is_scheduled[task_id]) for task_id in tasks))

    solver = cp_model.CpSolver()
    solver.parameters.max_time_in_seconds = 10
    solver.parameters.relative_gap_limit = 0.25

    # Medir tiempo de solución
    status = solver.Solve(model)
    solve_time = (datetime.now() - start_timestamp).total_seconds()

    # Procesar resultados
    task_results = {}
    makespan = 0
    total_task_group_duration = 0
    if status == cp_model.OPTIMAL or status == cp_model.FEASIBLE:
        for task_id, start_var in task_starts.items():
            scheduled = solver.Value(is_scheduled[task_id])
            start_time = solver.Value(start_var) if scheduled else None
            end_time = solver.Value(task_ends[task_id]) if scheduled else None
            if scheduled and end_time:
                makespan = max(makespan, end_time)
            task_results[task_id] = {
                "Scheduled": scheduled,
                "Start": (min_date + timedelta(hours=start_time / scaling)).isoformat() if scheduled else None
            }
    else:
        for task_id in tasks.keys():
            task_results[task_id] = {"Scheduled": 0, "Start": None}
            
    solve_time_full = (datetime.now() - start_timestamp).total_seconds()


    # Calcular valor de la función objetivo
    objective_value = solver.ObjectiveValue()

    # Escribir en el log
    log_solver_run(
        uuid4,
        tasks,
        task_results,
        solver.StatusName(status),
        solve_time,
        makespan / scaling,
        objective_value,
        solve_time_full
    )

    return task_results



def calculate_metrics(tasks_json, task_results):
    """
    Función para calcular métricas de desempeño del solver.
    """
    # Inicializar variables
    
    tasks_df, squads_df, tools_df = read_json(tasks_json)

    total_tasks = len(tasks_df)
    scheduled_tasks = sum(task_results[task_id]["Scheduled"] for task_id in task_results)
    unscheduled_tasks = total_tasks - scheduled_tasks
    
    solution = pd.DataFrame(task_results).T.reset_index().rename(columns={'index':'TaskID'})
    solution['Start'] = pd.to_datetime(solution['Start'])
    solution['TaskID'] = solution['TaskID'].astype(int)
    solution['Scheduled'] = solution['Scheduled'].astype(int)
    tasks_df = tasks_df.merge(solution[['TaskID', 'Start', 'Scheduled']], on='TaskID', how='left')
    tasks_df['Finish'] = tasks_df['Start'] + pd.to_timedelta(tasks_df['Duration'], unit='h')
    
    makespan = (tasks_df['Finish'].max() - tasks_df['Start'].min()).total_seconds() / 3600
    
    objective_value = sum((tasks_df['Impact'].max() + 1 - tasks_df['Impact']) ** 3 * (1 - tasks_df['Scheduled']))
    
    #calculate the average of the makespan of the group of tasks (OT column is the group of tasks)
    average_makespan = tasks_df.groupby('OT').apply(lambda x: (x['Finish'].max() - x['Start'].min()).total_seconds() / 3600).mean()
    return {
        "Total Tasks": total_tasks,
        "Scheduled Tasks": scheduled_tasks,
        "Unscheduled Tasks": unscheduled_tasks,
        "Makespan": makespan,
        "Objective Value": objective_value,
        "Average Makespan": round(average_makespan, 1)
    }



def solve2(tasks, task_windows, task_groups, max_impact, resource_capacities, resources_forbidden_intervals, min_date, scaling, uuid4):
    
    start_timestamp = datetime.now()

    # Definir el modelo de optimización
    model = cp_model.CpModel()
    is_scheduled = {}
    task_starts = {}
    task_ends = {}
    task_intervals = {}

    # Restricciones de Ventanas de Tiempo para las tareas
    for task_id, (duration, required_resources, cantidad, impact) in tasks.items():
        is_scheduled[task_id] = model.NewBoolVar(f'is_scheduled_{task_id}')
        start_var = model.NewIntVar(task_windows[task_id][0], task_windows[task_id][1] - duration, f'start_{task_id}')
        end_var = model.NewIntVar(task_windows[task_id][0] + duration, task_windows[task_id][1], f'end_{task_id}')
        interval_var = model.NewOptionalIntervalVar(start_var, duration, end_var, is_scheduled[task_id], f'interval_{task_id}')
        task_starts[task_id] = start_var
        task_ends[task_id] = end_var
        task_intervals[task_id] = interval_var

    # Añadir restricciones cumulativas para cada recurso
    for resource_id, (capacity, type) in resource_capacities.items():
        intervals = []
        demands = []
        for task_id, (duration, required_resources, cantidad, impact) in tasks.items():
            if resource_id in required_resources:
                intervals.append(task_intervals[task_id])
                if type == 0:
                    demands.append(cantidad)
                else:
                    demands.append(1)
        model.AddCumulative(intervals, demands, capacity)

    # Crear variables de programación para los grupos
    group_scheduled = {}
    group_makespans = {}
    for group_id, group in task_groups.items():
        group_var = model.NewBoolVar(f'group_scheduled_{group_id}')
        group_scheduled[group_id] = group_var

        # Asegurar que el estado programado de las tareas coincide con el del grupo
        for task_id in group:
            model.Add(is_scheduled[task_id] == group_var)
        
        # Calcular inicio y fin del grupo
        group_start = model.NewIntVar(min(task_windows[task_id][0] for task_id in group), 
                                      max(task_windows[task_id][1] for task_id in group), 
                                      f'group_start_{group_id}')
        group_end = model.NewIntVar(min(task_windows[task_id][0] for task_id in group), 
                                    max(task_windows[task_id][1] for task_id in group), 
                                    f'group_end_{group_id}')
        
        # Restringir inicio y fin a las tareas del grupo
        model.AddMinEquality(group_start, [task_starts[task_id] for task_id in group])
        model.AddMaxEquality(group_end, [task_ends[task_id] for task_id in group])
        
        # Calcular el makespan del grupo
        group_makespan = model.NewIntVar(0, max(task_windows[task_id][1] for task_id in group) - 
                                         min(task_windows[task_id][0] for task_id in group), 
                                         f'group_makespan_{group_id}')
        model.Add(group_makespan == group_end - group_start)
        group_makespans[group_id] = group_makespan

    # Calcular pesos inversos basados en 'Impact'
    weights = {task_id: (max_impact + 1 - tasks[task_id][3]) ** 3 for task_id in tasks}

    # Variable para el makespan global
    makespan = model.NewIntVar(0, sum(task_windows[task_id][1] for task_id in tasks), 'makespan')

    # Restricción del makespan global como el máximo de task_ends
    model.AddMaxEquality(makespan, [task_ends[task_id] for task_id in tasks])

    # Definir la función objetivo combinando minimización del makespan global, makespan de los grupos y ponderación
    alpha = 0  # Peso para el makespan global
    beta = 0   # Peso para los makespans de los grupos
    gamma = 1  # Peso para la ponderación de los impactos

    model.Minimize(
        alpha * makespan + 
        beta * sum(group_makespans[group_id] for group_id in group_makespans) + 
        gamma * sum(weights[task_id] * (1 - is_scheduled[task_id]) for task_id in tasks)
    )

    solver = cp_model.CpSolver()
    solver.parameters.max_time_in_seconds = 10
    solver.parameters.relative_gap_limit = 0.25

    # Medir tiempo de solución
    status = solver.Solve(model)
    solve_time = (datetime.now() - start_timestamp).total_seconds()

    # Procesar resultados
    task_results = {}
    calculated_makespan = 0
    group_makespan_results = {}
    if status == cp_model.OPTIMAL or status == cp_model.FEASIBLE:
        for task_id, start_var in task_starts.items():
            scheduled = solver.Value(is_scheduled[task_id])
            start_time = solver.Value(start_var) if scheduled else None
            end_time = solver.Value(task_ends[task_id]) if scheduled else None
            if scheduled and end_time:
                calculated_makespan = max(calculated_makespan, end_time)
            task_results[task_id] = {
                "Scheduled": scheduled,
                "Start": (min_date + timedelta(hours=start_time / scaling)).isoformat() if scheduled else None
            }
        for group_id in group_makespans:
            group_makespan_results[group_id] = solver.Value(group_makespans[group_id])
    else:
        for task_id in tasks.keys():
            task_results[task_id] = {"Scheduled": 0, "Start": None}
        for group_id in group_makespans.keys():
            group_makespan_results[group_id] = None

    solve_time_full = (datetime.now() - start_timestamp).total_seconds()

    # Calcular valor de la función objetivo
    objective_value = solver.ObjectiveValue()

    # Escribir en el log
    log_solver_run(
        uuid4,
        tasks,
        task_results,
        solver.StatusName(status),
        solve_time,
        calculated_makespan / scaling,
        objective_value,
        solve_time_full,
    )

    return task_results



