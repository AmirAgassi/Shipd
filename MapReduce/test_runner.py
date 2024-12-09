import os
import time
from signal import SIGTERM


def main():
    engine_ports = [9001, 9002, 9003]
    output_file_name = "output.txt"

    delete_file(file_name=output_file_name)
    kill_engine_processes(engine_ports=engine_ports)

    is_engine_start_up_success: bool = False
    is_driver_start_up_success: bool = False
    output_file_present: bool = False
    is_output_file_correct: bool = False

    is_engine_start_up_success = start_engines(engine_ports=engine_ports)
    start_time = time.time()
    is_driver_start_up_success = start_driver(engine_ports=engine_ports)
    end_time = time.time()

    print(f"Time taken to execute task: {end_time - start_time} seconds")
    if is_engine_start_up_success and is_driver_start_up_success:
        output_file_present = is_output_file_present(file_name=output_file_name)
        if output_file_present:
            is_output_file_correct = verify_output_file(output_file_name=output_file_name)

    kill_engine_processes(engine_ports=engine_ports)

    print("TEST SUMMARY")
    print("1. Engine instances start up:", ("SUCCESS" if is_engine_start_up_success else "FAIL"))
    print("2. Driver program start up:", ("SUCCESS" if is_driver_start_up_success else "FAIL"))
    print("3. Found output.txt file:", ("SUCCESS" if output_file_present else "FAIL"))
    print("4. is output file format correct:", ("SUCCESS" if is_output_file_correct else "FAIL"))


def delete_file(file_name: str):
    try:
        if os.path.exists(file_name):
            os.remove(file_name)
    except Exception as e:
        print(f"{file_name} not present")


def kill_engine_processes(engine_ports: list):
    try:
        for engine_port in engine_ports:
            command = f"kill -9 $(lsof -t -i:{engine_port})"
            print("command:", command)
            os.system(command)
    except Exception as e:
        print(e)


def start_engines(engine_ports: list):
    try:
        for engine_port in engine_ports:
            exit_code = os.system(f"./start_engine.sh {engine_port}")  # command to run engine at the given port

        return True
    except Exception as e:
        print(f"Failed to start engines at ports: {engine_ports}")
        return False


def start_driver(engine_ports: list):
    try:
        exit_code = os.system(
            f"./start_driver.sh {engine_ports[0]} {engine_ports[1]} {engine_ports[2]}")  # command to run driver at the given port
        return True
    except Exception as e:
        print(f"Failed to start driver")
        return False


def is_output_file_present(file_name: str):
    if os.path.exists(file_name):
        return True
    return False


def verify_output_file(output_file_name: str):
    with open(output_file_name, mode="r") as file:
        actual_output = file.read()

        expected_output = """2004,100,124,111
2005,125,149,136
2006,150,174,161
2007,175,199,186
2008,225,249,236
2009,250,274,261
2010,275,299,287
2011,300,324,311
2012,325,349,337
2013,350,374,362
2014,375,399,386
2015,400,424,412
2016,425,449,437
2017,450,474,462
2018,475,499,486
2019,500,549,524
2020,550,599,574
2021,600,649,624
2022,650,699,674
2023,700,749,724
2024,750,849,799
"""

        return actual_output == expected_output


if __name__ == "__main__":
    main()