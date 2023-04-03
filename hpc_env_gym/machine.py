import matplotlib.pyplot as plt
from datetime import datetime
from job import Job

class Machine():
    def __init__(self, node_name:str, # for identification purposes
                       total_mem:int, # in mb of memory this node has
                       total_cpus:int, # in number of cores this node has
                       total_gpus:int) -> None: # in number of gpus this node has
        self.node_name = node_name
        self.total_mem = total_mem
        self.avail_mem = total_mem
        self.total_cpus = total_cpus
        self.avail_cpus = total_cpus
        self.total_gpus = total_gpus
        self.avail_gpus = total_gpus
        self.running_jobs = []

        self.mem_util_pct = 0
        self.cpus_util_pct = 0
        self.gpus_util_pct = 0

        self.tick_times = []
        self.avail_mem_at_times = []
        self.avail_cpus_at_times = []
        self.avail_gpus_at_times = []

    def log_status(self, current_time):
        self.tick_times.append(current_time)
        self.avail_mem_at_times.append(self.avail_mem)
        self.avail_cpus_at_times.append(self.avail_cpus)
        self.avail_gpus_at_times.append(self.avail_gpus)
    
    def can_schedule(self, job):
        if job.req_mem <= self.avail_mem and job.req_cpus <= self.avail_cpus and job.req_gpus <= self.avail_gpus:
            return True
        else:
            return False
    
    def start_job(self, job):
        self.running_jobs.append(job)
        self.avail_mem -= job.req_mem
        self.avail_cpus -= job.req_cpus
        self.avail_gpus -= job.req_gpus

        self.update_pct_util()

        assert(self.avail_cpus >= 0), "Assigned {} to {} with insufficient cpu resources.".format(job, self.node_name)
        assert(self.avail_mem >= 0), "Assigned {} to {} with insufficient mem resources.".format(job, self.node_name)
        assert(self.avail_gpus >= 0), "Assigned {} to {} with insufficient gpu resources.".format(job, self.node_name)

    def stop_job(self, job_name:str):
        # remove a job and free up the resources it was using
        job_to_remove = None
        for index, job in enumerate(self.running_jobs):
            if job.job_name == job_name:
                job_to_remove = self.running_jobs[index]
                self.running_jobs = self.running_jobs[:index] + self.running_jobs[index+1:]
                break
        
        if job_to_remove == None:
            print("{} not found running on this machine".format(job_name))
        else:
            self.avail_mem +=  job_to_remove.req_mem
            self.avail_cpus += job_to_remove.req_cpus
            self.avail_gpus += job_to_remove.req_gpus

            self.update_pct_util()

            assert(self.avail_mem <= self.total_mem), "Machine {} stopped {}, and somehow has too much memory".format(self.node_name, job_to_remove)
            assert(self.avail_cpus <= self.total_cpus), "Machine {} stopped {}, and somehow has too many cpus".format(self.node_name, job_to_remove)
            assert(self.avail_gpus <= self.total_gpus), "Machine {} stopped {}, and somehow has too many GPUs".format(self.node_name, job_to_remove)
    
    def update_pct_util(self):
        
        self.mem_util_pct = (1 - self.avail_mem/self.total_mem)*100
        self.cpus_util_pct = (1 - self.avail_cpus/self.total_cpus)*100
        if self.total_gpus > 0: self.gpus_util_pct = (1 - self.avail_gpus/self.total_gpus)*100
        else: self.gpus_util_pct = 100

    def __repr__(self):
        s = "Machine("
        for key, value in self.__dict__.items():
            s += str(key) + "=" + repr(value) + ", "
        return s[:-2] + ")"
    
    def __str__(self):
        s = "Machine("
        for key, value in self.__dict__.items():
            s += str(key) + "=" + repr(value) + ", "
        return s[:-2] + ")"   