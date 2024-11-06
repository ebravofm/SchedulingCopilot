from pydantic import BaseModel
from typing import List, Optional

class Task(BaseModel):
    Task: int
    EarliestDate: int
    RequiredDate: int
    Squad: Optional[int] = None
    Tool: Optional[int] = None
    Len: float
    Q: int
    Impact: int
    TaskGroup: Optional[int] = None

class Squad(BaseModel):
    Cap: int
    Start: Optional[int] = 0
    ActiveHours: int
    InactiveHours: int
    ActiveDays: int
    InactiveDays: int

class InputData(BaseModel):
    tasks: List[Task]
    squads: List[Squad]

class TaskOutput(BaseModel):
    Task: int
    Scheduled: bool
    Start: Optional[int]
    End: Optional[int]

class OutputData(BaseModel):
    tasks: List[TaskOutput]
