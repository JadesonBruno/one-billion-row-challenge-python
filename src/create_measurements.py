import os
import sys
import random
import time

# Sanity checks out input and prints out usage if input is not a positive integer
def check_args(file_args):

    if len(file_args) != 2 or int(file_args[1]) <= 0:
        raise Exception("""Usage:  create_measurements.sh <positive integer number of records to create>
                                   You can use underscore notation for large number of records.
                                   For example:  1_000_000_000 for one billion""")

# Grabs the weather station names from example data provided in repo and dedups
def build_weather_station_name_list():

    station_names = []
    with open("./data/weather_stations.csv", "r", encoding="utf-8") as file:
        file_contents = file.read()
    for station in file_contents.splitlines():
        if "#" in station:
            continue
        else:
            station_names.append(station.split(';')[0])
    return list(set(station_names))

# Convert bytes to a human-readable format (e.g., KiB, MiB, GiB)
def convert_bytes(num):

    for x in ['bytes', 'KiB', 'MiB', 'GiB']:
        if num < 1024.0:
            return "%3.1f %s" % (num, x)
        num /= 1024.0

# Format elapsed time in a human-readable format
def format_elapsed_time(seconds):

    if seconds < 60:
        return f"{seconds:.3f} seconds"
    elif seconds < 3600:
        minutes, seconds = divmod(seconds, 60)
        return f"{int(minutes)} minutes {int(seconds)} seconds"
    else:
        hours, remainder = divmod(seconds, 3600)
        minutes, seconds = divmod(remainder, 60)
        if minutes == 0:
            return f"{int(hours)} hours {int(seconds)} seconds"
        else:
            return f"{int(hours)} hours {int(minutes)} minutes {int(seconds)} seconds"


#  Tries to estimate how large a file the test data will be
def estimate_file_size(weather_station_names, num_rows_to_create):

    max_string = float('-inf')
    min_string = float('inf')
    per_record_size = 0
    record_size_unit = "bytes"

    for station in weather_station_names:
        if len(station) > max_string:
            max_string = len(station)
        if len(station) < min_string:
            min_string = len(station)
        per_record_size = ((max_string + min_string * 2) + len(",-123.4")) / 2

    total_file_size = num_rows_to_create * per_record_size
    human_file_size = convert_bytes(total_file_size)

    return f"O tamanho estimado do arquivo é:  {human_file_size}.\nO tamanho final será provavelmente muito menor (metade)."

# Generates and writes to file the requested length of test data
def build_test_data(weather_station_names, num_rows_to_create):

    start_time = time.time()
    coldest_temp = -99.9
    hottest_temp = 99.9
    station_names_10k_max = random.choices(weather_station_names, k=10_000)
    batch_size = 10000 # instead of writing line by line to file, process a batch of stations and put it to disk
    progress_step = max(1, (num_rows_to_create // batch_size) // 100)
    print("Criando o arquivo... isso vai demorar uns 10 minutos...")

    try:
        with open("./data/measurements.txt", 'w', encoding="utf-8") as file:
            for s in range(0,num_rows_to_create // batch_size):
                
                batch = random.choices(station_names_10k_max, k=batch_size)
                prepped_deviated_batch = '\n'.join([f"{station};{random.uniform(coldest_temp, hottest_temp):.1f}" for station in batch]) # :.1f should quicker than round on a large scale, because round utilizes mathematical operation
                file.write(prepped_deviated_batch + '\n')
                
        sys.stdout.write('\n')
    except Exception as e:
        print("Something went wrong. Printing error info and exiting...")
        print(e)
        exit()
    
    end_time = time.time()
    elapsed_time = end_time - start_time
    file_size = os.path.getsize("./data/measurements.txt")
    human_file_size = convert_bytes(file_size)
 
    print("Arquivo escrito com sucesso data/measurements.txt")
    print(f"Tamanho final:  {human_file_size}")
    print(f"Tempo decorrido: {format_elapsed_time(elapsed_time)}")

# main program function
def main():

    check_args(sys.argv)
    num_rows_to_create = int(sys.argv[1])
    weather_station_names = build_weather_station_name_list()
    print(estimate_file_size(weather_station_names, num_rows_to_create))
    build_test_data(weather_station_names, num_rows_to_create)
    print("Arquivo de teste finalizado.")


if __name__ == "__main__":
    main()