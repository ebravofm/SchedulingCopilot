import pandas as pd
import os
os.environ["JAVA_HOME"]="/opt/miniconda3/envs/ortools/lib/jvm"
import jpype
import mpxj
import numpy as np

from java.lang import Double, Number
from java.time import LocalDate, LocalDateTime, DayOfWeek, LocalTime
from net.sf.mpxj import ProjectFile, TaskField, Duration, TimeUnit, RelationType, Availability, Relation, LocalTimeRange
from net.sf.mpxj.common import LocalDateTimeHelper
from net.sf.mpxj.writer import UniversalProjectWriter, FileFormat

def timestamp_to_LocalDateTime(timestamp):
    year = timestamp.year
    month = timestamp.month
    day = timestamp.day
    hour = timestamp.hour
    minute = timestamp.minute
        
    return LocalDateTime.of(year, month, day, hour, minute)


def generate_mspdi(tasks_df, squads_df, file_name = "Program.xml"):
    

    min_date = timestamp_to_LocalDateTime(tasks_df['Start'].min())

    df = tasks_df.copy()
    df = df[df.Scheduled == 1]

    file_format = FileFormat.MSPDI

    file = ProjectFile()

    calendar = file.addDefaultBaseCalendar()
    for day in DayOfWeek.values():
        calendar.setWorkingDay(day, True)
        hours = calendar.getCalendarHours(day)
        try:
            hours.clear()
        except:
            pass
        time_range = LocalTimeRange(LocalTime.MIDNIGHT, LocalTime.MIDNIGHT)
        hours.add(time_range)
        #print(hours)

    properties = file.getProjectProperties()
    properties.setStartDate(min_date)

    # Add Resources
    personal = df['Squad'].dropna().unique().tolist()
    equipment = df['Tool'].dropna().unique().tolist()
    personal_resources = {}
    equipment_resources = {}

    for p in personal:
        cap = float(squads_df[squads_df['Name'] == p]['Capacity'].iloc[0]*100)
        personal_resources[p] = file.addResource()
        personal_resources[p].setName(p)
        personal_resources[p].getAvailability().add(Availability(LocalDateTimeHelper.START_DATE_NA, LocalDateTimeHelper.END_DATE_NA, Double.valueOf(cap)));
        personal_resources[p].setPeakUnits(cap)
    for e in equipment:
        equipment_resources[e] = file.addResource()
        equipment_resources[e].setName(e)


    ots = {}
    tasks = {}
    subtasks = {}
    # Iterate over distinct values of "OT" column, and then iterate over the rows of the dataframe
    for ot in df['OT'].unique():
        tasks[ot] = {}
        subtasks[ot] = {}
        ots[ot] = file.addTask()
        ots[ot].setName(f"{ot}: {df[df['OT'] == ot]['OTDescription'].iloc[0]}")
        
        for i, row in df[df['OT'] == ot].iterrows():
            tasks[ot][i] = ots[ot].addTask()
            tasks[ot][i].setName(f"{row['Task']}: {row['TaskDescription']}")

            tasks[ot][i].setDuration(Duration.getInstance(row['Duration'], TimeUnit.HOURS))
            tasks[ot][i].setStart(timestamp_to_LocalDateTime(row['Start']))
            tasks[ot][i].setFinish(timestamp_to_LocalDateTime(row['Finish']))

            tasks[ot][i].setPercentageComplete(Double.valueOf(0))
            tasks[ot][i].setActualStart(timestamp_to_LocalDateTime(row['Start']))
            tasks[ot][i].setActualFinish(timestamp_to_LocalDateTime(row['Finish']))
            
            tasks[ot][i].addResourceAssignment(personal_resources[row['Squad']])
            if not pd.isna(row['Tool']):
                tasks[ot][i].addResourceAssignment(equipment_resources[row['Tool']])

    # Verificar posibilidad de hacerlo en RAM
    writer = UniversalProjectWriter(file_format).write(file, file_name)

    # Leer el contenido del archivo
    with open(file_name, "r", encoding="utf-8") as file:
        content = file.read()
    updated_content = content.replace("<Manual>0</Manual>", "<Manual>1</Manual>")
    with open(file_name, "w", encoding="utf-8") as file:
        file.write(updated_content)

