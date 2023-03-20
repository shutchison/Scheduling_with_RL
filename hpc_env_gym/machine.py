import matplotlib.pyplot as plt
from datetime import datetime

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

        self.tick_times = []
        self.avail_mem_at_times = []
        self.avail_cpus_at_times = []
        self.avail_gpus_at_times = []

    def log_status(self, current_time):
        self.tick_times.append(current_time)
        self.avail_mem_at_times.append(self.avail_mem)
        self.avail_cpus_at_times.append(self.avail_cpus)
        self.avail_gpus_at_times.append(self.avail_gpus)

    def start_job(self, job):
        self.running_jobs.append(job)
        self.avail_mem -= job.req_mem
        self.avail_cpus -= job.req_cpus
        self.avail_gpus -= job.req_gpus

        assert(self.avail_cpus >= 0)
        assert(self.avail_mem >= 0)
        assert(self.avail_gpus >= 0)

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

            assert(self.avail_mem <= self.total_mem)
            assert(self.avail_cpus <= self.total_cpus)
            assert(self.avail_gpus <= self.total_gpus)
            
    def plot_usage(self, model_type):
        fig = plt.figure(figsize=[12,10])
        fig.suptitle("Utilization for {}".format(self.node_name))
        mem_perc = [mem/self.total_mem for mem in self.avail_mem_at_times]
        cpu_perc = [cpu/self.total_cpus for cpu in self.avail_cpus_at_times]
        if self.total_gpus != 0:
            gpu_perc = [mem/self.total_gpus for mem in self.avail_gpus_at_times]
        else:
            gpu_perc = [0 for _ in self.avail_gpus_at_times]
        ticks = [datetime.fromtimestamp(t) for t in self.tick_times]
        
        plt.plot(ticks,
                 mem_perc, 
                 color="red", 
                 label="Memory Utilization")

        plt.plot(ticks, 
                 gpu_perc, 
                 color="blue", 
                 label="GPU Utilization")

        plt.plot(ticks, 
                cpu_perc, 
                color="green", 
                label="CPU Utilization")
        plt.legend()
        
        plt.xlabel("time")
        plt.ylabel("Percentage utilized")
        plt.savefig("plots/{}_{}.jpg".format(self.node_name, model_type), bbox_inches="tight")
        plt.close(fig)
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
    