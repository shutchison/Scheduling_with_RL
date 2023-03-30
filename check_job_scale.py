import numpy as np
import pandas as pd


USE_TEST_DATA = False

csv_file_name = "all_jobs_2021.csv" if not USE_TEST_DATA else "hpc_env_gym/jobs.csv"

df = pd.read_csv(csv_file_name)
f = open(csv_file_name)
lines = f.readlines()
f.close()

header = lines[0]
lines = lines[1:]

max_mem = -1
max_cpus = -1
max_gpus = -1

for line in lines:
    elements = line.split(",")
    req_mem = int(elements[1])
    req_cpu = int(elements[2])
    req_gpu = int(elements[3])

    if req_mem > max_mem: max_mem = req_mem
    if req_cpu > max_cpus: max_cpus = req_cpu
    if req_gpu > max_gpus: max_gpus = req_gpu

print("\nMemory Requests")
print("Max: {}".format(max_mem))
print("Avg: {}".format(round(np.mean(df.loc[:,"ReqMem"]),2)))
print("Std: {}".format(round(np.std(df.loc[:,"ReqMem"]),2)))
print("Percentiles: 33% - {}\t66% - {}".format(df["ReqMem"].quantile(.33),df["ReqMem"].quantile(.66)))
print("\nCPU Requests")
print("Max: {}".format(max_cpus))
print("Avg: {}".format(round(np.mean(df.loc[:,"ReqCPUs"]),2)))
print("Std: {}".format(round(np.std(df.loc[:,"ReqCPUs"]),2)))
print("Percentiles: 33% - {}\t66% - {}".format(df["ReqCPUs"].quantile(.33),df["ReqCPUs"].quantile(.66)))
print("\nGPU Requests")
print("Max: {}".format(max_gpus))
print("Avg: {}".format(round(np.mean(df.loc[:,"ReqGPUs"]),2)))
print("Std: {}".format(round(np.std(df.loc[:,"ReqGPUs"]),2)))
print("Percentiles: 33% - {}\t66% - {}".format(df["ReqGPUs"].quantile(.33),df["ReqGPUs"].quantile(.66)))



csv_file_name = "beocat.csv" if not USE_TEST_DATA else "hpc_env_gym/machines.csv"

df = pd.read_csv(csv_file_name)
f = open(csv_file_name)
lines = f.readlines()
f.close()

header = lines[0]
lines = lines[1:]

max_mem = -1
max_cpus = -1
max_gpus = -1

for line in lines:
    elements = line.split(",")
    avail_mem = int(elements[1])
    avail_cpu = int(elements[2])
    avail_gpu = int(elements[3])

    if avail_mem > max_mem: max_mem = avail_mem
    if avail_cpu > max_cpus: max_cpus = avail_cpu
    if avail_gpu > max_gpus: max_gpus = avail_gpu

print("\nMemory Available")
print("Max: {}".format(max_mem))
print("Avg: {}".format(round(np.mean(df.loc[:,"TotalMemory"]),2)))
print("Std: {}".format(round(np.std(df.loc[:,"TotalMemory"]),2)))
print("Percentiles: 33% - {}\t66% - {}".format(df["TotalMemory"].quantile(.33),df["TotalMemory"].quantile(.66)))
print("\nCPUs Available")
print("Max: {}".format(max_cpus))
print("Avg: {}".format(round(np.mean(df.loc[:,"TotalCPUs"]),2)))
print("Std: {}".format(round(np.std(df.loc[:,"TotalCPUs"]),2)))
print("Percentiles: 33% - {}\t66% - {}".format(df["TotalCPUs"].quantile(.33),df["TotalCPUs"].quantile(.66)))
print("\nGPUs Available")
print("Max: {}".format(max_gpus))
print("Avg: {}".format(round(np.mean(df.loc[:,"TotalGPUs"]),2)))
print("Std: {}".format(round(np.std(df.loc[:,"TotalGPUs"]),2)))
print("Percentiles: 33% - {}\t66% - {}".format(df["TotalGPUs"].quantile(.33),df["TotalGPUs"].quantile(.66)))