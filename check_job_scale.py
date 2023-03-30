
csv_file_name = "all_jobs_2021.csv"

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

print("Max mem req: {}".format(max_mem))
print("Max cpu req: {}".format(max_cpus))
print("Max gpu req: {}".format(max_gpus))

csv_file_name = "beocat.csv"

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

print("Max mem avail: {}".format(max_mem))
print("Max cpu avail: {}".format(max_cpus))
print("Max gpu avail: {}".format(max_gpus))