import queue
from pprint import pprint
from machine import Machine
from job import Job
from datetime import datetime
import csv
import logging
from cluster import Cluster

class Scheduler():
    def __init__(self) -> None: # what scheduling method to use
        self.cluster = Cluster()
        self.global_clock = 0
        self.clock_offset = 0

        #initialize self.future_jobs with all jobs we need to run
        self.future_jobs = queue.PriorityQueue()  # ordered based on submit time
        self.job_queue = []
        self.running_jobs = queue.PriorityQueue() # ordered based on end time
        self.completed_jobs = []
    
    def reset(self, machines_csv, jobs_csv):
        """
        Loads the machine from the machines_csv file and the jobs from the jobs_csv file.
        Removes the first job from future jobs and puts it in the jobs_queue, and updates the global clock
        """
        self.load_cluster(machines_csv)
        self.load_jobs(jobs_csv)
        self.global_clock, job = self.future_jobs.get()
        self.clock_offset = self.global_clock
        self.job_queue.append(job)
    
    def load_cluster(self, csv_file_name):
        self.cluster.load_machines(csv_file_name)

    def bbf(self, job):
        return self.cluster.best_bin_first(job, self.global_clock)
    
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
        
    def get_schedulable_jobs(self):
        schedulable_jobs = []
        for job in self.job_queue:
            for machine in self.cluster.machines:
                if machine.can_schedule(job):
                    schedulable_jobs.append(job)
                    break
        if len(schedulable_jobs) == 0:
            schedulable_jobs = None
        return schedulable_jobs

    def get_shortest_schedulable_job(self):
        # locates the shortest schedulable job in the job_queue, if any, pops is off the queue and returns it.
        schedulable_jobs = self.get_schedulable_jobs()
        if schedulable_jobs is None:
            return None
        else:
            shortest_job = sorted(schedulable_jobs, key=lambda x: x.req_duration)[0]
            for index, j in enumerate(self.job_queue):
                if j == shortest_job:
                    return self.job_queue.pop(index)
        return None

    def get_best_bin_first_machine(self, job):
        min_fill_margin = 10
        assigned_machine = None
        assigned_machine_index = None
        for machine_index, m in enumerate(self.cluster.machines):
            if (m.avail_mem >= job.req_mem) and (m.avail_cpus >= job.req_cpus) and (m.avail_gpus >= job.req_gpus):
                #print("checking {}".format(m.node_name))
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
                #print("{} has fill margin {}".format(m.node_name, fill_margin))
                if fill_margin < min_fill_margin:
                    min_fill_margin = fill_margin
                    assigned_machine = m
                    assigned_machine_index = machine_index
                    
        return (assigned_machine_index, assigned_machine)

    def tick(self):
        # tick will advance time until at least one job can be scheduled, but will
        # do no scheduling itself.

        # returns True if the state can schedule more jobs, or False if there 
        # is nothing else for the simulation to do.
        
        print("global clock: {:,}".format(self.global_clock))
        # iterate through self.future_jobs to find all jobs with job.submission_time == self.global_clock
        # move these jobs to self.job_queue ordered based on job.submision_time
        # iterate through self.running jobs and remove all jobs from machines whose job.end_time == self.global_clock
        # append these jobs to self.completed_jobs
        # iterate through self.job_queue and attempt to schedule all jobs using appropriate algorithm
        # move successfully scheduled jobs to the self.running_jobs

        # Need the minimum time between submit and ending
        # In either of these events, see if we can schedule more jobs
        # If we can, call schedule to do so
        # if not, keep calling tick again until we can.  This should cause 
        # when we return from this function, a job can be scheduled.
        
        if self.get_schedulable_jobs() is not None:
            # There are jobs which can still be scheduled.  We don't need to advance the clock any since there is
            # still work which can be done.
            return True
        
        keep_going = True
        first_submit = 1e100
        first_end = 1e100
        while keep_going:
            if not self.future_jobs.empty():
                first_submit = self.future_jobs.queue[0][0]
            else:
                first_submit = 1e100
            if not self.running_jobs.empty():
                first_end = self.running_jobs.queue[0][0]
            else:
                first_end = 1e100

            self.global_clock = min([first_submit, first_end])
            print("Updating global clock to {:,}".format(self.global_clock))
            if self.global_clock == first_submit:
                # move jobs who have been submitted now into the job_queue
                while not self.future_jobs.empty():
                    current_first_submit = self.future_jobs.queue[0][0]
                    if current_first_submit > self.global_clock:
                        break
                    elif current_first_submit == self.global_clock:
                        job = self.future_jobs.get()[1]
                        print("{} submitted at time {:,}".format(job.job_name, self.global_clock))
                        self.job_queue.append(job)
            
            if self.global_clock == first_end:
                # stop all jobs who end at the current time and move them to completed
                while not self.running_jobs.empty():
                    current_first_end = self.running_jobs.queue[0][0]
                    if current_first_end > self.global_clock:
                        break
                    elif current_first_end == self.global_clock:
                        end_time, job = self.running_jobs.get()
                        found = False
                        for m in self.cluster.machines:
                            for j in m.running_jobs:
                                if job.job_name == j.job_name:
                                    print("job {} ending at time {}".format(job.job_name, self.global_clock))
                                    found = True
                                    m.stop_job(job.job_name)
                                    self.completed_jobs.append(job)
                                    #self.machines_log_status()
                                    #self.log_training_data_csv(job, self.machines, m.node_name, "Stop")
                                    break
                            if found:
                                break
                        if not found:
                            print("Error!  Failed to find and end {}".format(job.job_name))
            
            if self.get_schedulable_jobs() is not None:
                keep_going = False

        if self.is_simulation_complete():
            return False
        
        return True
    
    def is_simulation_complete(self):
        # returns True if there is nothing left to do.
        # False otherwise
        if self.future_jobs.empty() and self.running_jobs.empty() and self.get_schedulable_jobs() is None:
                print("No future jobs, no running jobs, and no more jobs I can schedule!  Nothing left to do.")
                print(self.summary_statistics())
                return True
            
        if self.global_clock == 1e100:
            print("Global clock has gone to infinity.  Something went wrong or we're done?")
            print(self.summary_statistics())
            return True
        
        return False

    def summary_statistics(self):
        s = "="*40 + "\n"
        s += "global logical clock = {:,}\n".format(self.global_clock - self.clock_offset)
        #s += "num future jobs = {}, {}\n".format(len(self.future_jobs.queue), self.future_jobs.queue)
        #s += "num queued jobs = {}, {}\n".format(len(self.job_queue), self.job_queue)
        #s += "num running jobs = {}, {}\n".format(len(self.running_jobs.queue), self.running_jobs.queue)
        s += "num future jobs = {}\n".format(len(self.future_jobs.queue))
        s += "num queued jobs = {}\n".format(len(self.job_queue))
        s += "num running jobs = {}\n".format(len(self.running_jobs.queue))
        s += "num completed jobs = {}\n".format(len(self.completed_jobs))
        s += "="*40
        
        return s
        
    def run_job(self, job, machine_index):
        job.start_time = self.global_clock
        job.end_time = job.start_time + job.actual_duration
        self.cluster.machines[machine_index].start_job(job)
        self.running_jobs.put( (job.end_time, job) )
        
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
