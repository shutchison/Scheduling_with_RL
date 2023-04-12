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
        self.schedulable_jobs = []
    
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
        return schedulable_jobs

    def jobs_can_be_scheduled(self):
        for job in self.job_queue:
            for machine in self.cluster.machines:
                if machine.can_schedule(job):
                    return True
        return False

    def get_shortest_schedulable_job_index(self):
        # locates the shortest schedulable job in the job_queue, if any, and returns its index
        schedulable_jobs = self.get_schedulable_jobs()
        if len(schedulable_jobs) <= 0:
            return None, None
        else:
            shortest_job = sorted(schedulable_jobs, key=lambda x: x.req_duration)[0]
            for index, job in enumerate(self.job_queue):
                if job == shortest_job:
                    return index, job

    def pop_shortest_schedulable_job(self):
        # pops shortest schedulable job and returns it
        index, job = self.get_shortest_schedulable_job_index()
        if index is not None:
            return self.job_queue.pop(index)
        else:
            return None

    def peek_shortest_schedulable_job(self):
        # peeks shortest schedulable job
        index, job = self.get_shortest_schedulable_job_index()
        return job

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
        # tick will advance time until at least one job can be scheduled or ended,
        # and then it will queue and/or end those jobs.
        # It will do no scheduling itself.

        # returns True when the state can schedule more jobs or False if there 
        # is nothing else for the simulation to do.
        
        # Gets the minimum time value between the next job submission and/or job end (both could happen at once)
        # Updates the simulation state depending on which action(s) are taken
        # Then sees if we can schedule more jobs
        # If not, repeats the process until we can
        # This should create a system state such that upon return, a job can be scheduled.
        
        print("global clock: {:,}".format(self.global_clock))

        # If there are no more tasks for the simulation, return.
        if self.simulation_is_complete():
            return False

        # If there are still schedulable jobs, don't update the system state.
        if self.jobs_can_be_scheduled():
            return True
        
        # Move time to the next event, update the system state, check for schedulable jobs, possibly repeat
        advancing_time = True
        while advancing_time:

            next_job_submit_time = 1e100
            next_job_end_time = 1e100
            if not self.future_jobs.empty(): next_job_submit_time = self.future_jobs.queue[0][0] 
            if not self.running_jobs.empty(): next_job_end_time = self.running_jobs.queue[0][0]

            self.global_clock = min([next_job_submit_time, next_job_end_time])
            print("Updating global clock to {:,}".format(self.global_clock))

            if self.global_clock == next_job_submit_time: self.tick_queue_jobs()
            if self.global_clock == next_job_end_time: self.tick_end_jobs()
            
            if self.jobs_can_be_scheduled():
                advancing_time = False
        
        if self.simulation_is_complete():
            return False

        return True

    def tick_queue_jobs(self):
        # iterate through self.future_jobs to find all jobs with job.submission_time == self.global_clock
        # move these jobs to self.job_queue ordered based on job.submision_time
        while not self.future_jobs.empty():
            submit_time = self.future_jobs.queue[0][0]
            if submit_time > self.global_clock:
                break
            elif submit_time <= self.global_clock:
                new_time, new_job = self.future_jobs.get()
                print("{} submitted at time {:,}".format(new_job.job_name, self.global_clock))
                self.job_queue.append(new_job)

    def tick_end_jobs(self):
        # iterate through self.running jobs and remove all jobs from machines whose job.end_time == self.global_clock
        # append these jobs to self.completed_jobs
        while not self.running_jobs.empty():
            end_time = self.running_jobs.queue[0][0]
            if end_time > self.global_clock:
                break
            elif end_time <= self.global_clock:
                old_time, ending_job = self.running_jobs.get()
                found_job = False
                for machine in self.cluster.machines:
                    for running_job in machine.running_jobs:
                        if ending_job.job_name == running_job.job_name:
                            print("job {} ending at time {}".format(ending_job.job_name, self.global_clock))
                            found_job = True
                            machine.stop_job(ending_job.job_name)
                            self.completed_jobs.append(ending_job)
                            #self.machines_log_status()
                            #self.log_training_data_csv(job, self.machines, m.node_name, "Stop")
                            break
                    if found_job:
                        break
                if not found_job:
                    print("Error! Failed to find and end {}".format(ending_job.job_name))

    def simulation_is_complete(self):
        # returns True if there is nothing left to do.
        # False otherwise
        sim_complete = False
        if self.future_jobs.empty() and self.running_jobs.empty() and not self.jobs_can_be_scheduled():
            print("No future jobs, no running jobs, and no more jobs I can schedule!  Nothing left to do.")
            print(self.summary_statistics())
            sim_complete = True
            
        if self.global_clock == 1e100:
            print("Global clock has gone to infinity.  Something went wrong or we're done?")
            print(self.summary_statistics())
            sim_complete = True
        
        return sim_complete

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
