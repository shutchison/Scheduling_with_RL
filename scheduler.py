import queue
from pprint import pprint
from machine import Machine
from job import Job

class Scheduler():
    def __init__(self, model_type:str) -> None: # what scheduling method to use
        self.machines = []
        self.global_clock = 0
        self.model_type = model_type

        #initialize self.future_jobs with all jobs we need to run
        self.future_jobs = queue.PriorityQueue()  # ordered based on submit time
        self.job_queue = queue.PriorityQueue()    # ordered based on submit time
        self.running_jobs = queue.PriorityQueue() # ordered based on end time
        self.completed_jobs = []
    
    def load_machines(self, csv_file_name):
        f = open(csv_file_name)
        lines = f.readlines()
        f.close()
        lines = list(map(str.strip, lines))
        # print(lines)
        headers = lines[0]
        for line in lines[1:]:
            elements = line.split(",")
            self.machines.append(Machine(elements[0], *map(int, elements[1:])))
    
    def load_jobs(self, csv_file_name):
        f = open(csv_file_name)
        lines = f.readlines()
        f.close()
        lines = list(map(str.strip, lines))
        # print(lines)
        headers = lines[0]
        for line in lines[1:]:
            elements = line.split(",")
            j = Job(elements[0], *map(int, elements[1:]))
            # wrap this in a tuple, so they are ordered by their sumbission time.
            self.future_jobs.put( (j.submission_time, j) )
        
        # Initialize global clock to submission time of first job
        self.global_clock = self.future_jobs.queue[0][0]

    def tick(self):
        # iterate through self.future_jobs to find all jobs with job.submision_time == self.global_clock
        # move these jobs to self.job_queue ordered based on job.submision_time
        # iterate through self.running jobs and remove all jobs from machines whose job.end_time == self.global_clock
        # append these jobs to self.completed_jobs
        # iterate through self.job_queue and attempt to schedule all jobs using appropriate algorithm
        # move successfully scheduled jobs to the self.running_jobs

        while not self.future_jobs.empty() or not self.running_jobs.empty():

            for rj in range(len(self.running_jobs.queue)):
                if self.running_jobs.queue[rj][1].end_time == self.global_clock:
                    self.completed_jobs.put(self.running_jobs.get())

            if self.future_jobs.queue[0][0] == self.global_clock:
                self.job_queue.put(self.future_jobs.get())
            else:
                while not self.job_queue.empty():
                    jq = self.job_queue.get()
                    print("Scheduling " + jq[1].job_name)
                    success = self.schedule(jq[1])
                    self.running_jobs.put(jq)
                    if not success:
                        print("No machine available for " + jq[1].job_name)
                        self.future_jobs.put(jq)

                self.global_clock = min(self.future_jobs.queue[0][0], self.running_jobs.queue[0][1].end_time)
        
    def schedule(self, job):
        if self.model_type == "PPG":
            return self.PPG(job) #TODO: load this saved model upon object creation

        elif self.model_type == "DDPG":
            return self.DDPG(job) #TODO: load this saved model upon object creation

        elif self.model_type == "SJF":
            return self.shortest_job_first(job) # Shortest job first will minimize avg job queue time, but can cause starvation.

        elif self.model_type == "BBF":
            return self.best_bin_first(job)
        else:
            # default bin packing procedure best bin first
            return self.best_bin_first(job)


    def shortest_job_first(self, job):
        return False
    #starvation

    def best_bin_first(self, job):
        min_fill_margin = 1e100
        assigned_machine = None
        for m in self.machines:
            if (m.avail_mem >= job.req_mem) and (m.avail_cpus >= job.req_cpus) and (m.avail_gpus >= job.req_gpus):
                # TODO: Should constraint margins be scaled for this calc? Mem is on the scale of 1e9 and cpus/gpus are on the scale of 1.
                fill_margin = (m.avail_mem - job.req_mem) + (m.avail_cpus - job.req_cpus) + (m.avail_gpus - job.req_gpus)
                if fill_margin <= min_fill_margin:
                    min_fill_margin = fill_margin
                    assigned_machine = m
        if assigned_machine is not None:
            self.set_job_time(job)
            assigned_machine.start_job(job)
            return True
        else:
            return False

    def PPG(self, job):
        return False
        # call self.PPG.predict (or whatever the API says to do) to decide where to schedule this job
        # recall we have three contraints to satisfy: machine must have adequate memory, cpus, and gpus for this job
        # call self.set_job_time to record its time span
        # call machine.start_job to start the job running

    def DDPG(self, job):
        return False
        # call self.DDPG.predict (or whatever the API says to do) to decide where to schedule this job
        # recall we have three contraints to satisfy: machine must have adequate memory, cpus, and gpus for this job
        # call self.set_job_time to record its time span
        # call machine.start_job to start the job running

    def set_job_time(self, job):
        job.start_time = self.global_clock
        job.end_time = self.global_clock + job.actual_duration

    def calculate_metrics(self) -> float:
        # iterate through self.completed_jobs and compute the avg queue time for all jobs which have been compelted
        return 3.1415