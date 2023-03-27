import hpc_env_gym.envs.hpc_env 
import gym


env = gym.make("HPCEnv-v0", render_mode="human")

options = {"machines_csv" : "machines.csv",
           "jobs_csv" : "jobs.csv"}

env.reset(options=options)

#for i in range(200):
#    env.step()
    