
from functools import total_ordering

# total ordering allows for auto generation of all comparison operators
# for comparing one job to another job.
# Equality will check if the job_name strings are the same
# less than, greater than, etc. will use ReqDuration
@total_ordering
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

    def __eq__(self, other):
        if type(other) != type(self):
            return False
        return self.job_name == other.job_name

    def __lt__(self, other):
        return self.req_duration < other.req_duration