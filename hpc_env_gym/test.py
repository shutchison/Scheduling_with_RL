import hpc_env_gym.envs.hpc_env 
import gym
from pprint import pprint
import random
from time import sleep

env = gym.make("HPCEnv-v0", render_mode="human")

options = {"machines_csv" : "machines.csv",
           "jobs_csv" : "jobs.csv"}

env.reset(options=options)

for i in range(10000):
    print("="*40)
    print("Step {}".format(i))
    print("="*40)
    node_to_sched = random.randint(0,7)
    observation, reward, terminated, truncated, info = env.step(node_to_sched)
    num_in_queue = len(observation["job_queue"])
    pprint(("queue_depth = {}".format(num_in_queue), observation["machines"], reward, terminated, truncated, info))
    env.render()
    
    
    if terminated:
        print("Terminated")
        break
    if truncated:
        print("Truncated")
        break
    