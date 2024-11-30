import pandas as pd
import os
os.environ["JAVA_HOME"]="/opt/miniconda3/envs/ortools/lib/jvm"
import jpype

import mpxj
import numpy as np
from utils import read_json, load_solution
from lxml import etree


from java.lang import Double, Number, String
from java.time import LocalDate, LocalDateTime, DayOfWeek, LocalTime
from net.sf.mpxj import ProjectFile, TaskField, Duration, TimeUnit, RelationType, Availability, Relation, LocalTimeRange
from net.sf.mpxj.common import LocalDateTimeHelper
from net.sf.mpxj.writer import UniversalProjectWriter, FileFormat

from java.io import ByteArrayOutputStream
from java.nio.charset import StandardCharsets



def timestamp_to_LocalDateTime(timestamp):
    year = timestamp.year
    month = timestamp.month
    day = timestamp.day
    hour = timestamp.hour
    minute = timestamp.minute
        
    return LocalDateTime.of(year, month, day, hour, minute)


def generate_mspdi(tasks_json_path='tasks.json', solver_json_path=None, file_name = "solution.xml"):
        
    tasks_df, squads_df, tools_df = read_json(tasks_json_path)

    if solver_json_path is None:
        tasks_df['Start'] = tasks_df['Start(p)']
        tasks_df['Scheduled'] = np.where(tasks_df['Start'].notnull(), 1, 0)
    else:
        solution = load_solution(solver_json_path)
        tasks_df = tasks_df.merge(solution[['TaskID', 'Start', 'Scheduled']], on='TaskID', how='left')
            
    tasks_df['Finish'] = tasks_df['Start'] + pd.to_timedelta(tasks_df['Duration'], unit='h')

    min_date = timestamp_to_LocalDateTime(tasks_df['Start'].min())

    df = tasks_df.copy()
    df = df[df.Scheduled == 1]

    df = df.sort_values(by=['Start'])

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


    personal = squads_df[squads_df['SquadID'].isin(df['SquadID'].dropna().unique().tolist())]['Name'].tolist()
    equipment = tools_df[tools_df['ToolID'].isin(df['ToolID'].dropna().unique().tolist())]['Name'].tolist()
    personal_resources = {}
    equipment_resources = {}


    for p in personal:
        cap = float(squads_df[squads_df['Name'] == p]['Capacity'].iloc[0]*100)
        personal_resources[p] = file.addResource()
        personal_resources[p].setName(p)
        personal_resources[p].getAvailability().add(Availability(LocalDateTimeHelper.START_DATE_NA, LocalDateTimeHelper.END_DATE_NA, Double.valueOf(cap)));
        personal_resources[p].setPeakUnits(cap)
        #personal_resources[p].setCalendar(calendar)

    for e in equipment:
        equipment_resources[e] = file.addResource()
        equipment_resources[e].setName(e)
        #equipment_resources[e].setPeakUnits(1)
        #equipment_resources[e].setCalendar(calendar)



    ots = {}
    tasks = {}
    subtasks = {}
    # Iterate over distinct values of "OT" column, and then iterate over the rows of the dataframe
    for ot in df['OT'].unique():
        tasks[ot] = {}
        subtasks[ot] = {}
        ots[ot] = file.addTask()
        ots[ot].setName(f"{ot}: {df[df['OT'] == ot]['OTDescription'].iloc[0]}")

        max_end_ot = df[df['OT'] == ot]['Finish'].iloc[0]
        min_start_ot = df[df['OT'] == ot]['Start'].iloc[0]
        
        for i, row in df[df['OT'] == ot].iterrows():
            #print(row)
            tasks[ot][i] = ots[ot].addTask()
            tasks[ot][i].setName(f"{row['Task']}: {row['TaskDescription']}")

            tasks[ot][i].setDuration(Duration.getInstance(row['Duration'], TimeUnit.HOURS))
            tasks[ot][i].setStart(timestamp_to_LocalDateTime(row['Start']))
            tasks[ot][i].setFinish(timestamp_to_LocalDateTime(row['Finish']))

            tasks[ot][i].setPercentageComplete(Double.valueOf(0))
            tasks[ot][i].setActualStart(timestamp_to_LocalDateTime(row['Start']))
            tasks[ot][i].setActualFinish(timestamp_to_LocalDateTime(row['Finish']))
            
            tasks[ot][i].addResourceAssignment(personal_resources[squads_df[squads_df['SquadID']==row['SquadID']]['Name'].iloc[0]])
            if not pd.isna(row['ToolID']):
                tasks[ot][i].addResourceAssignment(equipment_resources[tools_df[tools_df['ToolID']==row['ToolID']]['Name'].iloc[0]])
            #print(tasks[ot][i].getDuration().toString())
            
            max_end_ot = row['Finish'] if row['Finish'] > max_end_ot else max_end_ot
            min_start_ot = row['Start'] if row['Start'] < min_start_ot else min_start_ot
            
        ots[ot].setStart(timestamp_to_LocalDateTime(min_start_ot))
        ots[ot].setFinish(timestamp_to_LocalDateTime(max_end_ot))
        ots[ot].setDuration(Duration.getInstance((max_end_ot - min_start_ot).total_seconds()/3600, TimeUnit.HOURS))




    output_stream = ByteArrayOutputStream()
    writer = UniversalProjectWriter(file_format)
    writer.write(file, output_stream)  # Asegúrate de que `file` es ProjectFile
    project_bytes = output_stream.toByteArray()  # Los datos en formato de bytes
    project_text = String(project_bytes, StandardCharsets.UTF_8)  # Decodificar usando la codificación UTF-8
    output_stream.close()

    # Parsear el XML
    python_text = str(project_text)

    if python_text.startswith("<?xml"):
        python_text = python_text.split("?>", 1)[1].strip()

    root = etree.fromstring(python_text)

    # Definir namespaces (ajusta esto según tu XML si es necesario)
    namespaces = {'ns': root.nsmap[None]} if None in root.nsmap else {}

    # Encontrar todas las tareas
    tasks = root.xpath("//ns:Task", namespaces=namespaces) if namespaces else root.xpath("//Task")

    for task in tasks:
        
        # Buscar y modificar el tag Manual
        manual = task.find("ns:Manual", namespaces) if namespaces else task.find("Manual")
        if manual is not None:
            manual.text = "1"  # Cambiar el valor de Manual a 1
            
        # Buscar valores de Start, Finish y Duration
        start = task.find("ns:Start", namespaces) if namespaces else task.find("Start")
        finish = task.find("ns:Finish", namespaces) if namespaces else task.find("Finish")
        duration = task.find("ns:Duration", namespaces) if namespaces else task.find("Duration")

        # Agregar nuevas etiquetas relacionadas con Start
        if start is not None:
            for tag_name in ["ManualStart", "EarlyStart", "LateStart"]:
                new_tag = etree.SubElement(task, tag_name)
                new_tag.text = start.text

        # Agregar nuevas etiquetas relacionadas con Finish
        if finish is not None:
            for tag_name in ["ManualFinish", "EarlyFinish", "LateFinish"]:
                new_tag = etree.SubElement(task, tag_name)
                new_tag.text = finish.text

        # Agregar nuevas etiquetas relacionadas con Duration
        if duration is not None:
            for tag_name in [
                "ManualDuration", "Work", "ActualDuration", "ActualWork",
                "ActualOvertimeWork", "RegularWork", "RemainingDuration", "RemainingWork"
            ]:
                new_tag = etree.SubElement(task, tag_name)
                new_tag.text = duration.text


    # Convertir el árbol XML modificado a una cadena
    modified_xml = etree.tostring(root, pretty_print=True, encoding="utf-8", xml_declaration=True)

    # Guardar el resultado en un archivo
    with open(file_name, "wb") as file:
        file.write(modified_xml)
