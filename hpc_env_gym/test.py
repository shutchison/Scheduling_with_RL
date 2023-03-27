import hpc_env_gym.envs.hpc_env 
import gym
from pprint import pprint

env = gym.make("HPCEnv-v0", render_mode="human")

options = {"machines_csv" : "machines.csv",
           "jobs_csv" : "jobs.csv"}

env.reset(options=options)
pprint(env.step(0))

#for i in range(200):
#    env.step()
    