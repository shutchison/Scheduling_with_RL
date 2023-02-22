from scheduler import Scheduler
from machine import Machine
from job import Job
from pprint import pprint

s = Scheduler(model_type="oracle")
s.load_machines("machines.csv")
s.load_jobs("jobs.csv")
i = 0

while True:
    # i += 1
    # print("="*40 + "\ni={}".format(i))
    if not s.tick():
        break
print("="*40)
# pprint(s.completed_jobs)
print("{} future jobs".format(len(s.future_jobs.queue)))
print("{} jobs pending".format(len(s.job_queue)))
print("{} jobs running".format(len(s.running_jobs.queue)))
print("{} jobs completed".format(len(s.completed_jobs)))
print("Avg queue time is {} seconds".format(s.calculate_metrics()))

for m in s.machines:
    m.plot_usage()

print("done")
