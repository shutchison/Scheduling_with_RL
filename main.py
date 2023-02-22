from scheduler import Scheduler
from machine import Machine
from job import Job
from pprint import pprint

s = Scheduler(model_type="bbf")
s.load_machines("machines.csv")
s.load_jobs("jobs.csv")
while True:
    if not s.tick():
        break
print("="*40)
print("For best fit first")
print("Avg queue time is {} seconds".format(s.calculate_metrics()))
s.machines_plot()

s = Scheduler(model_type="sjf")
s.load_machines("machines.csv")
s.load_jobs("jobs.csv")
while True:
    if not s.tick():
        break
print("="*40)
print("For shortest job first")
print("Avg queue time is {} seconds".format(s.calculate_metrics()))
s.machines_plot()

s = Scheduler(model_type="oracle")

s.load_machines("machines.csv")
s.load_jobs("jobs.csv")
while True:
    if not s.tick():
        break
print("="*40)
print("For oracle")
print("Avg queue time is {} seconds".format(s.calculate_metrics()))
s.machines_plot()


print("done")
