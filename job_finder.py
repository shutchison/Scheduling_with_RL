from collections import namedtuple
from datetime import datetime
from datetime import timedelta
from pprint import pprint
import pandas as pd
import numpy as np

# f = open("all_jobs_2021_part1.csv", "r")
# lines = f.readlines()
# f.close()
# lines = list(map(str.strip, lines))
# 
# headers = lines[0].split(",")
# print(headers)
# 
# Job = namedtuple("Job", headers)
# jobs = []
# 
# for line in lines[1:]:
    # jobs.append(Job(*line.split(",")))
#     
# f = open("all_jobs_2021_part2.csv", "r")
# lines = f.readlines()
# f.close()
# lines = list(map(str.strip, lines))
# for line in lines[1:]:
    # jobs.append(Job(*line.split(",")))
# 
# print("{} jobs found".format(len(jobs)))
# 
# pprint(jobs[:10])

#part1 = pd.read_csv("all_jobs_2021_part1.csv")
#part2 = pd.read_csv("all_jobs_2021_part2.csv")
#jobs = pd.concat([part1, part2])

jobs = pd.read_csv("all_jobs_2021.csv")
print("{} jobs found".format(len(jobs)))
print(jobs[:10])

delta = timedelta(days=31)

# Can specify date times like this if you want hours: "2021-01-04T00:05:23"
start_str = "2021-01-01"
start_date = datetime.fromisoformat(start_str)
end_date = start_date + delta
for i in range(11):
    start_timestamp = start_date.timestamp()
    end_timestamp = (start_date + delta).timestamp()
    
    # keep jobs between start and end dates
    job_filter = jobs.iloc[np.where((jobs.Submit >= start_timestamp) & (jobs.Submit <= end_timestamp))]
    print("{} jobs found between {} and {}".format(len(job_filter), start_date, end_date))
    
    job_filter.to_csv("all_jobs_2021{:02}.csv".format(start_date.month), index=False)
    
    
    start_date = end_date
    end_date = start_date + delta





