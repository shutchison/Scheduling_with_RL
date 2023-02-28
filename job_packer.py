import numpy as np
import pandas as pd
from datetime import datetime
import matplotlib.pyplot as plt

def pack_jobs(job_csv_file, start_date_iso_format, num_days_to_pack):
    # usage: 
    # inputs: job_csv_file - the CSV file you want to open,
    #         start_date_iso_format - a date string in iso format
    #         num_days_to_pack - the number of days you'd like to pack these jobs into
    # returns: Nothing
    # side effects: Writes an output file with the packed file
    #
    # Example usage:
    # pack_jobs("all_jobs_202101.csv", "2021-02-01", 7)
    
    num_secs = num_days_to_pack*24*60*60
    start_date = datetime.fromisoformat(start_date_iso_format)
    start_timestamp = start_date.timestamp()
    
    jobs = pd.read_csv(job_csv_file)
    submit_times = [j for j in jobs["Submit"]]
    
    hist, bins = np.histogram(submit_times, bins=num_secs)

    jobs["Bins"] = np.digitize(jobs["Submit"], bins)
    jobs["Submit"] = start_timestamp + jobs["Bins"]
    jobs = jobs.astype({"Submit": "int"})
    jobs = jobs.drop("Bins", axis=1)
    
    outfile_name = "packed_{}_".format(num_days_to_pack) + job_csv_file
    jobs.to_csv(outfile_name, index=False)
    print("output written to {}".format(outfile_name))    

pack_jobs("all_jobs_202101.csv", "2021-02-01", 7)