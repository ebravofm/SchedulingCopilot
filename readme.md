# Scheduling-Copilot

Assistant for scheduling maintenance tasks.

## Overview

Scheduling-Copilot is a tool designed to assist in the scheduling of maintenance tasks. It automates the complex and time-consuming process of task scheduling, reducing errors and improving efficiency. By generalizing the scheduling problem into a mathematical model, the tool leverages optimization algorithms to find optimal solutions.

## Features

- **Automated Scheduling**: Provides an initial feasible schedule automatically.
- **Resource Optimization**: Efficiently allocates resources like squads and equipment.
- **Gantt Chart Output**: Generates a Gantt chart for visualizing the task schedule.
- **Constraint Handling**: Considers time windows, resource capacities, and task precedence.
- **Priority Management**: Prioritizes tasks based on criticality and impact.

## How It Works

Scheduling-Copilot models the scheduling problem as a **Resource-Constrained Project Scheduling Problem (RCPSP)**. Once modeled, it uses optimization tools like **CP-SAT** to find optimal or near-optimal solutions.

### Model Parameters

- **Tasks**: Each task includes:
  - Earliest Start Date
  - Required Completion Date
  - Duration
  - Required Squad
  - Quantity of Workers Needed
  - Required Equipment/Workshops
  - Criticality
  - Precedence Relations

- **Resources**:
  - **Squads**:
    - Number of Workers
    - Shift Pattern (e.g., 7x7D)
    - Working Hours per Day
    - Shift Start Date
  - **Equipment**:
    - Availability Windows

### Decision Variables

- **Task Scheduling**: Binary variables indicating if a task is scheduled.
- **Start Times**: Variables representing the start time of each task.

### Constraints

1. **Time Windows**: Tasks must start and finish within their allowed time frames.
2. **Resource Capacities**: The sum of resource demands at any time cannot exceed resource capacities.
3. **Forbidden Intervals**: Tasks cannot be scheduled during resources' unavailable times.

### Objective Function

The objective is to minimize penalties associated with not scheduling higher-priority tasks. This is achieved by assigning weights to tasks based on their impact:

\[
\text{Weight}_i = (\text{Impact}_{\text{max}} + 1 - \text{Impact}_i)^3
\]

The function heavily penalizes the non-scheduling of higher-priority tasks, optimizing the overall criticality of the schedule.
