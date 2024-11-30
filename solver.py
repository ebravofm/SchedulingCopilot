import logging
from datetime import datetime, timedelta
from ortools.sat.python import cp_model

# Configuración básica del logger
logging.basicConfig(
    filename="solver.log",
    level=logging.INFO,
    format="%(message)s",
    filemode="a"  # Abrir en modo append
)

def log_solver_run(
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


def solve(tasks, task_windows, task_groups, max_impact, resource_capacities, resources_forbidden_intervals, min_date, scaling):
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
    for group_id, group in task_groups.items():
        group_var = model.NewBoolVar(f'group_scheduled_{group_id}')
        group_scheduled[group_id] = group_var
        for task_id in group:
            model.Add(is_scheduled[task_id] == group_var)

    # Calcular pesos inversos basados en 'Impact'
    weights = {task_id: (max_impact + 1 - tasks[task_id][3]) ** 3 for task_id in tasks}

    # Definir la función objetivo ponderada
    model.Minimize(sum(weights[task_id] * (1 - is_scheduled[task_id]) for task_id in tasks))

    solver = cp_model.CpSolver()
    solver.parameters.max_time_in_seconds = 10
    solver.parameters.relative_gap_limit = 0.25

    # Medir tiempo de solución
    start_timestamp = datetime.now()
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
        tasks,
        task_results,
        solver.StatusName(status),
        solve_time,
        makespan / scaling,
        objective_value,
        solve_time_full
    )

    return task_results
