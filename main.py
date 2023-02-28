from scheduler import Scheduler

s = Scheduler(model_type="oracle")
s.conduct_simulation("machines.csv", "jobs.csv")
