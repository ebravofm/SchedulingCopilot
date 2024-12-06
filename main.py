import jpype
jpype.startJVM()
import json
import argparse
from msproject import generate_mspdi
from solver import run_solver
from utils import print_excel

# Inicializa la JVM

def main():
    # Configuración de argumentos para el CLI
    parser = argparse.ArgumentParser(description="Run solver and export results.")
    parser.add_argument("-s", "--split-tasks", action="store_true", help="Enable splitting tasks during solving.")
    parser.add_argument("-p", "--export-to-xml", action="store_true", help="Generate a Microsoft Project XML file.")
    parser.add_argument("-x", "--export-to-xlsx", action="store_true", help="Generate an Excel file.")
    parser.add_argument("-t", "--task-file", type=str, default="tasks.json", help="Path to the task file (default: tasks.json).")
    parser.add_argument("-o", "--output-file", type=str, default="solution.json", help="Path to save the JSON output (default: solution.json).")
    parser.add_argument("-P", "--xml-file", type=str, default="solution.xml", help="Path for the XML output (default: solution.xml).")
    parser.add_argument("-X", "--xlsx-file", type=str, default="solution.xlsx", help="Path for the Excel output (default: solution.xlsx).")
    args = parser.parse_args()

    # Ejecuta el solver
    print("Running solver...")
    task_results = run_solver(args.task_file, split_tasks=args.split_tasks)
    print("Solving complete.")

    # Guarda los resultados en un archivo JSON
    with open(args.output_file, "w") as json_file:
        json.dump(task_results, json_file, indent=4)
    print(f"Results saved to {args.output_file}")

    # Exporta a XML si es solicitado
    if args.export_to_xml:
        generate_mspdi(args.task_file, args.output_file, args.xml_file)
        print(f"Microsoft Project file generated as {args.xml_file}")

    # Exporta a Excel si es solicitado
    if args.export_to_xlsx:
        print_excel(args.task_file, args.output_file, args.xlsx_file)
        print(f"Excel file generated as {args.xlsx_file}")

if __name__ == "__main__":
    main()
