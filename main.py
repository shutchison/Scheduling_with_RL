from scheduler import Scheduler
from machine import Machine
from job import Job
from pprint import pprint

j = Job(job_name="job",
        req_mem=4000,
        req_cpus=1,
        req_gpus=2, 
        req_duration=10, 
        actual_duration=8, 
        submission_time=1)

m = Machine(node_name="compute_node_1",
            total_mem=64000000,
            total_cpus=64,
            total_gpus=4)


#print(m)

s = Scheduler(model_type="BBF")
s.load_machines("machines.csv")
s.load_jobs("jobs.csv")
#pprint(s.future_jobs.queue)
s.tick()
for m in s.machines:
    print(m)

