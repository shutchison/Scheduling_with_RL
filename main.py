from scheduler import Scheduler

s = Scheduler(model_type="bbf")
s.conduct_simulation("beocat.csv", "all_jobs_202101.csv")
