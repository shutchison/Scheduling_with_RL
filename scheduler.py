import queue

class Job():
    def __init__(self, job_name:str, # For identification purposes
                       req_mem:int, # In megabytes of memory requested
                       req_cpus:int, # in number of cpus requested
                       req_gpus:int, # in number of 
                       req_duration:int, # in seconds
                       actual_duration:int, # in seconds
                       submission_time:int) -> None: # by the time step when this job will be submitted

        self.job_name = job_name
        self.req_mem = req_mem
        self.req_cpus = req_cpus
        self.req_gpus = req_gpus
        self.req_duration = req_duration
        self.actual_duration = actual_duration
        self.submission_time = submission_time
        self.start_time = None
        self.end_time = None

    def __repr__(self):
        s = "Job("
        for key, value in self.__dict__.items():
            s += str(key) + "=" + repr(value) + ", "
        return s[:-2] + ")"
    
    def __str__(self):
        s = "Job("
        for key, value in self.__dict__.items():
            s += str(key) + "=" + repr(value) + ", "
        return s[:-2] + ")"

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

    def schedule(self, job):
        self.running_jobs.append(job)
        self.avail_mem -= job.req_mem
        self.avail_cpus -= job.req_cpus
        self.avail_gpus -= job.req_gpus

        assert(self.avail_cpus >= 0)
        assert(self.avail_mem >= 0)
        assert(self.avail_gpus >= 0)

    def stop_job(self):
        # remove a job and free up the resources it was using
        pass
        
        
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
    

class Schdueler():
    def __init__(self):
        self.machines = []
        self.global_clock = 0
        self.PPG_model = None #TODO: load this saved model upon object creation
        self.DDPG_model = None #TODO:  load this saved model upon object creation

        #initialize self.future_jobs with all jobs we need to run
        self.future_jobs = queue.PriorityQueue() # ordered based on submit time
        self.queue = queue.PriorityQueue() # ordered based on submit time
        self.running_jobs = queue.PriorityQueue() # ordered based on end time
        self.completed_jobs = []

    def tick(self):
        self.global_clock += 1
        # iterate through self.future_jobs to find all jobs with job.submit_time == self.global_clock
        # move these jobs to self.queue ordered based on job.submit_time
        # iterate through self.running jobs and remove all jobs from machins whose job.end_time == self.global_clock
        # append these jobs to self.completed_jobs
        # iterate through self.queue and attempt to schedule all jobs using appropriate algorithm
        # move successfully scheduled jobs to the self.running_jobs
        
    def best_bin_first(self, job):
        pass
        # look at self.machines, decide which machine can run this job using best bin first algorithm,
        # recall we have three contraints to satisfy: machine must have adequate memory, cpus, and gpus for this job
        # call machine.schedule to start the job running
        # update job.start_time to be self.global_clock
        # update job.end_time to be self.global_clock + job.actual_duration

    def PPG(self, job):
        pass
        # call self.PPG.predict (or whatever the API says to do) to decide where to schedule this job
        # recall we have three contraints to satisfy: machine must have adequate memory, cpus, and gpus for this job
        # call machine.schedule to start the job running
        # update job.start_time to be self.global_clock
        # update job.end_time to be self.global_clock + job.actual_duration

    def DDPG(self, job):
        pass
        # call self.DDPG.predict (or whatever the API says to do) to decide where to schedule this job
        # recall we have three contraints to satisfy: machine must have adequate memory, cpus, and gpus for this job
        # call machine.schedule to start the job running
        # update job.start_time to be self.global_clock
        # update job.end_time to be self.global_clock + job.actual_duration

    def calculate_metrics(self) -> float:
        # iterate through self.completed_jobs and compute the avg queue time for all jobs which have been compelted
        return 3.1415
