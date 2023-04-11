import hpc_env_gym.envs.hpc_env 
import gym
from pprint import pprint
import random
from time import sleep
from job import Job

env = gym.make("HPCEnv-v0", render_mode="human")

options = {"machines_csv" : "machines.csv",
           "jobs_csv" : "jobs.csv"}

env.reset(options=options)

dummy_job = Job("job",
                env.scheduler.job_queue[0].req_mem,
                env.scheduler.job_queue[0].req_cpus,
                env.scheduler.job_queue[0].req_gpus,
                env.scheduler.job_queue[0].req_duration,
                0,
                env.scheduler.global_clock)

def update_dummy_job(obs):
    return Job("job",
                obs[0][0]*1000000,
                obs[0][1],
                obs[0][2],
                obs[0][3]*3600,
                0,
                0)

def get_bbf_node_to_schedule(sched, dummy_job):
    node_index, node = sched.get_best_bin_first_machine(dummy_job)
    return node_index


for i in range(10000):
    print("Step #{}".format(i))
    
    node_to_sched = random.randint(0,7)
    node_to_sched = get_bbf_node_to_schedule(env.scheduler, dummy_job)
    
    observation, reward, terminated, truncated, info = env.step(node_to_sched)
    
    print("observation is: ")
    pprint(observation)
    print("="*60)
    
    # Toggle visualization by commenting this out
    env.render()
    
    dummy_job = update_dummy_job(observation)