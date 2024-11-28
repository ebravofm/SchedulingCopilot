from ortools.sat.python import cp_model
from datetime import timedelta


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
                    demands.append(cantidad)  # Cada tarea usa 1 unidad de capacidad
                else:
                    demands.append(1)  # Cada tarea usa 1 unidad de capacidad
        # Capacidad del recurso establecida a 1
        model.AddCumulative(intervals, demands, capacity)


    # Crear variables de programación para los grupos (Precedencia dentro de TaskGroup)
    group_scheduled = {}
    # Restricciones de TaskGroup
    for group_id, group in task_groups.items():
        # Crear una variable booleana para el grupo
        group_var = model.NewBoolVar(f'group_scheduled_{group_id}')
        group_scheduled[group_id] = group_var

        # Vincular las variables 'is_scheduled' de las tareas con la variable del grupo
        for task_id in group:
            model.Add(is_scheduled[task_id] == group_var)

        # Agregar restricciones de precedencia entre tareas consecutivas dentro del grupo
        previous_task_id = None
        for task_id in group:
            if previous_task_id is not None:
                # Si el grupo está programado, asegurar la precedencia entre tareas
                model.Add(task_starts[task_id] >= task_starts[previous_task_id]+tasks[previous_task_id][0]).OnlyEnforceIf(group_var)
            previous_task_id = task_id
            
    # Calcular pesos inversos basados en 'Impact' (1 es más prioritario)
    weights = {task_id: (max_impact + 1 - tasks[task_id][3]) ** 3 for task_id in tasks}

    # Definir la función objetivo ponderada
    model.Minimize(sum(weights[task_id] * (1 - is_scheduled[task_id]) for task_id in tasks))

    solver = cp_model.CpSolver()
    #solver.parameters.log_search_progress = True
    solver.parameters.max_time_in_seconds = 10  # Establece el límite de tiempo a 60 segundos (ajusta según tus necesidades)
    solver.parameters.relative_gap_limit = 0.25  # Define un gap aceptable del 25%
    
    
    #solver.parameters.stop_after_first_solution = True
    #solver.parameters.search_branching = cp_model.FIXED_SEARCH  
      
    #print(1)
    status = solver.Solve(model)
    
    # Verificar si se encontró una solución
    if status == cp_model.OPTIMAL or status == cp_model.FEASIBLE:
        task_results = {}
        #print('Solución Encontrada.')

        # Iterar sobre TaskIDs para recopilar resultados
        for task_id in task_starts.keys():  # task_starts debe estar definido previamente
            scheduled = solver.Value(is_scheduled[task_id])
            start_time = solver.Value(task_starts[task_id]) if scheduled else None  # Solo tiene sentido si está programada
            
            task_results[task_id] = {
                "Scheduled": scheduled,
                "Start": (min_date + timedelta(hours=start_time/scaling)).isoformat() if scheduled else None
            }
    else:
        #print('No se encontró una solución factible.')
        for task_id in tasks.keys():  # task_starts debe estar definido previamente            
            task_results[task_id] = {
                "Scheduled": 0,
                "Start": None
            }
        
    return task_results
