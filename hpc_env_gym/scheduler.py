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
        self.job_queue.append(job)
    
    def load_cluster(self, csv_file_name):
        self.cluster.load_machines(csv_file_name)
    
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
