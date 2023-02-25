import queue
from pprint import pprint
from machine import Machine
from job import Job
from datetime import datetime

class Scheduler():
    def __init__(self, model_type:str) -> None: # what scheduling method to use
        self.machines = []
        self.global_clock = 0
        self.model_type = model_type

        #initialize self.future_jobs with all jobs we need to run
        self.future_jobs = queue.PriorityQueue()  # ordered based on submit time
        self.job_queue = []
        self.running_jobs = queue.PriorityQueue() # ordered based on end time
        self.completed_jobs = []
    
    def conduct_simulation(self, machines_csv, jobs_csv):
        self.load_machines(machines_csv)
        self.load_jobs( jobs_csv)
        start_time = datetime.now()
        while True:
            if not self.tick():
                break
        end_time = datetime.now()
        print("Simulation complete.  Simulation time: {}".format(end_time-start_time))
        
    def machines_log_status(self):
        for m in self.machines:
            m.log_status(self.global_clock)
    
    def machines_plot(self):
        for m in self.machines:
            m.plot_usage(self.model_type)
    
    def reset(self, model_type:str):
        #chart_generation
        self.machines = []
        self.global_clock = 0
        self.model_type = model_type

        self.future_jobs = queue.PriorityQueue()  # ordered based on submit time
        self.job_queue = []
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
        # initialize global clock to be the submission time of the first job
        self.global_clock = self.future_jobs.queue[0][0]

    def tick(self):
        # iterate through self.future_jobs to find all jobs with job.submission_time == self.global_clock
        # move these jobs to self.job_queue ordered based on job.submision_time
        # iterate through self.running jobs and remove all jobs from machines whose job.end_time == self.global_clock
        # append these jobs to self.completed_jobs
        # iterate through self.job_queue and attempt to schedule all jobs using appropriate algorithm
        # move successfully scheduled jobs to the self.running_jobs

        # move jobs who have been submitted now into the job_queue
        while not self.future_jobs.empty():
            first_submit = self.future_jobs.queue[0][0]
            if first_submit > self.global_clock:
                break
            elif first_submit == self.global_clock:
                job = self.future_jobs.get()[1]
                #print("{} submitted at time {}".format(job.job_name, self.global_clock))
                self.job_queue.append(job)

        # stop all jobs who end at the current time and move them to completed
        while not self.running_jobs.empty():
            first_end = self.running_jobs.queue[0][0]
            if first_end > self.global_clock:
                break
            elif first_end == self.global_clock:
                end_time, job = self.running_jobs.get()
                found = False
                for m in self.machines:
                    for j in m.running_jobs:
                        if job.job_name == j.job_name:
                            #print("job {} ending at time {}".format(job.job_name, self.global_clock))
                            found = True
                            m.stop_job(job.job_name)
                            self.completed_jobs.append(job)
                            self.machines_log_status()
                            break
                    if found:
                        break

        # Depending on scheduling algorithm, sort by job duration
        if self.model_type == "sjf":
            self.job_queue.sort(key=lambda x: x.req_duration) # sjf = shortest job first by requested duration
        if self.model_type == "oracle":
            self.job_queue.sort(key=lambda x: x.actual_duration) # oracle = shortest job first with knowledge of actual duration
        
        # Try to schedule any jobs which can now be run
        none_can_be_scheduled = False
        while not none_can_be_scheduled:
            any_scheduled = False
            for index, job in enumerate(self.job_queue):
                scheduled = self.schedule(job)
                if scheduled:
                    # print("job {} started at time {}".format(job.job_name, self.global_clock))
                    any_scheduled = True
                    self.running_jobs.put( (job.end_time, job) )
                    self.job_queue = self.job_queue[:index] + self.job_queue[index+1:]
                    self.machines_log_status()
                    break
            if not any_scheduled:
                none_can_be_scheduled = True

        # update global clock to be the next submisison or ending event
        if self.future_jobs.empty() and self.running_jobs.empty():
            print("No future jobs and no running jobs!")
            return False

        first_submit = 1e100
        first_end = 1e100
        if not self.future_jobs.empty():
            first_submit = self.future_jobs.queue[0][0]
        if not self.running_jobs.empty():
            first_end = self.running_jobs.queue[0][0]
        
        self.global_clock = min(first_submit, first_end)

        if self.global_clock == 1e100:
            print("Something has gone wrong updating the global clock.")
            return False

        return True
        
    def schedule(self, job):
        if self.model_type == "ppg":
            return self.PPG(job) #TODO: load this saved model upon object creation

        elif self.model_type == "ddpg":
            return self.DDPG(job) #TODO: load this saved model upon object creation

        elif self.model_type == "sjf":
            return self.shortest_job_first(job)

        elif self.model_type == "bbf":
            return self.best_bin_first(job)

        elif self.model_type == "oracle":
            return self.shortest_job_first(job)

        else:
            # default bin packing procedure best bin first
            return self.best_bin_first(job)


    def shortest_job_first(self, job):
        # jobs are sorted by ascending duration before being handed to this function
        assigned_machine = None
        for m in self.machines:
            if (m.avail_mem >= job.req_mem) and (m.avail_cpus >= job.req_cpus) and (m.avail_gpus >= job.req_gpus):
                assigned_machine = m
        if assigned_machine is not None:
            self.set_job_time(job)
            assigned_machine.start_job(job)
            return True
        else:
            return False
        

    def best_bin_first(self, job):
        min_fill_margin = 10
        assigned_machine = None
        for m in self.machines:
            if (m.avail_mem >= job.req_mem) and (m.avail_cpus >= job.req_cpus) and (m.avail_gpus >= job.req_gpus):
                
                # not all nodes have both GPUs and CPUs, so init each margin to 0
                mem_margin = 0.0
                cpu_margin = 0.0
                gpu_margin = 0.0

                # count how many attributes the node has to normalize the final margin
                n_attributes = 0

                if m.total_mem > 0:
                    mem_margin = (m.avail_mem - job.req_mem)/m.total_mem
                    n_attributes += 1

                if m.total_cpus > 0:
                    cpu_margin = (m.avail_cpus - job.req_cpus)/m.total_cpus
                    n_attributes += 1

                if m.total_gpus > 0:
                    gpu_margin = (m.avail_gpus - job.req_gpus)/m.total_gpus
                    n_attributes += 1

                if n_attributes == 0:
                    print("{} has no virtual resources configured (all <= 0).".format(m.node_name))
                    fill_margin = 10
                else:
                    fill_margin = (mem_margin + cpu_margin + gpu_margin)/n_attributes

                if fill_margin < min_fill_margin:
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
        queue_sum = sum([job.start_time-job.submission_time for job in self.completed_jobs])
        if len(self.completed_jobs) != 0:
            return queue_sum/len(self.completed_jobs)
        else:
            print("There are no completed jobs!")
            return 3.1415

    def __repr__(self):
        s = "Scheduler("
        for key, value in self.__dict__.items():
            if type(value) == queue.PriorityQueue:
                s += str(value.queue)
            else:
                s += str(key) + "=" + repr(value) + ",\n"
        return s[:-2] + ")"
    
    def __str__(self):
        s = "Scheduler("
        for key, value in self.__dict__.items():
            if type(value) == queue.PriorityQueue:
                s += str(key) + "=" + repr(value.queue) + ",\n"
            else:
                s += str(key) + "=" + repr(value) + ",\n"
        return s[:-2] + ")"
