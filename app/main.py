from fastapi import FastAPI
from app.schemas import InputData, OutputData
from app.models import schedule_tasks

app = FastAPI()

@app.post("/schedule", response_model=OutputData)
def schedule(input_data: InputData):
    return schedule_tasks(input_data)
