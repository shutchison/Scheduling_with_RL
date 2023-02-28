from scheduler import Scheduler

s = Scheduler(model_type="oracle")
s.conduct_simulation("beocat.csv", "packed_7_all_jobs_202101.csv")