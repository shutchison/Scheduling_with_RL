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
                0)

def update_dummy_job(obs):
    return Job("job",
                obs[0]*1000000,
                obs[1],
                obs[2],
                obs[3]*3600,
                0,
                0)

# Really sketchy enum
RANDOM = 1
BBF = 2
ORACLE = 3 # Shortest Job Next based on actual duration
SJN = 4 # Shortest Job Next based on requested duration

DECISION_MODE = ORACLE
decision_name = ""

for i in range(10000):
    print("Step #{}".format(i))
   
    if DECISION_MODE == RANDOM:
        node_to_sched = random.randint(0,7)
        decision_name = "Random"
    elif DECISION_MODE == BBF:
        node_to_sched, node = env.scheduler.get_best_bin_first_machine(dummy_job)
        decision_name = "Best Bin First"
    elif DECISION_MODE == ORACLE:
        env.scheduler.use_oracle = True
        node_to_sched, node = env.scheduler.get_first_available_machine(dummy_job)
        decision_name = "Oracle"
    elif DECISION_MODE == SJN:
        node_to_sched, node = env.scheduler.get_first_available_machine(dummy_job)
        decision_name = "Shortest Job Next"
    else:
        node_to_sched = random.randint(0,7)
   
    observation, reward, terminated, truncated, info = env.step(node_to_sched)

    print("observation is: ")
    pprint(observation)
    print("="*60)
   
    # Toggle visualization by commenting this out
    #env.render()
   
    dummy_job = update_dummy_job(observation)
    if terminated or truncated:
        print(f"Terminated is {terminated}")
        print(f"truncated is {truncated}")
        break

average_queue_time = env.scheduler.get_avg_job_queue_time()

print("Machines File: {}".format(env.scheduler.machines_file))
print("Jobs File: {}".format(env.scheduler.jobs_file))
print("Decision method: {}".format(decision_name))
print("Average Queue Time: {} hours".format(round(average_queue_time/3600,2)))