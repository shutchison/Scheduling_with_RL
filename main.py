from scheduler import Job, Machine, Schdueler

j = Job(job_name="job1",
        req_mem=4000,
        req_cpus=1,
        req_gpus=2, 
        req_duration=10, 
        actual_duration=8, 
        submission_time=1)

print(j)

m = Machine(node_name="compute_node_1",
            total_mem=64000000,
            total_cpus=64,
            total_gpus=4)
print(m)
m.schedule(j)
print(m)