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
    print("Step #{}".format(i))
    node_to_sched = random.randint(0,7)
    observation, reward, terminated, truncated, info = env.step(node_to_sched)
    print("observation is: ")
    pprint(observation)
    print("="*60)
    env.render()
    
    
#     if terminated:
#         print("Terminated")
#         break
#     if truncated:
#         print("Truncated")
#         break
    