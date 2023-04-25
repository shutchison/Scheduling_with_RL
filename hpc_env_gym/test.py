import hpc_env_gym.envs.hpc_env 
import gym
from pprint import pprint
import random
from job import Job
import torch
import time

# ===========================================
#             Init the environment
# ===========================================
options = {}

env = gym.make("HPCEnv-v0", render_mode="human")
env.reset(options=options)

# ===========================================
#        Init input to decision models
# ===========================================
dummy_job = Job("job",
                env.scheduler.job_queue[0].req_mem,
                env.scheduler.job_queue[0].req_cpus,
                env.scheduler.job_queue[0].req_gpus,
                env.scheduler.job_queue[0].req_duration,
                0,
                0)

observation = env.scheduler._get_obs()

# ===========================================
#               Init RL model
# ===========================================
agent_file_5k = "actor_5k.pt"
agent_file_3M = "actor_3M.pt"
model_file = r"C:\Projects\Scheduling_with_RL_models" + "\\"

agent_5k = torch.load(model_file + agent_file_5k, map_location=torch.device("cpu"))
agent_3M = torch.load(model_file + agent_file_3M, map_location=torch.device("cpu"))
num_agent_corrections = 0
# ===========================================
#         Init decision model options
# ===========================================
# Really sketchy enum
RANDOM = 1
BBF = 2
ORACLE = 3 # Shortest Job Next based on actual duration
SJN = 4 # Shortest Job Next based on requested duration
PPO = 5
PPO2 = 6
decision_name = ""

DECISION_MODE = PPO

decision_list = [ORACLE, BBF, PPO, PPO2]

num_trials = 5

print("Machines File: {}".format(env.scheduler.machines_file))
print("Jobs File: {}".format(env.scheduler.jobs_file))
if PPO in decision_list:
    print("Agent file: {}".format(agent_file_5k))
if PPO2 in decision_list:
    print("Agent file: {}".format(agent_file_3M))
print("\nRunning {} trials for {} decision methods...".format(num_trials, len(decision_list)))
with open("trial_results.csv", "w") as csvfile:

    csvfile.write("Machines File: {}\n".format(env.scheduler.machines_file))
    csvfile.write("Jobs File: {}\n".format(env.scheduler.jobs_file))
    if PPO in decision_list:
        csvfile.write("Agent file: {}\n".format(agent_file_5k))
    if PPO2 in decision_list:
        csvfile.write("Agent file: {}\n".format(agent_file_5k))
    
    if PPO in decision_list or PPO2 in decision_list:
        csvfile.write("\nTrial,Decision_Method,AQT_sec,Simulation_Time_sec,Num_agent_corrections\n")
    else:
        csvfile.write("\nTrial,Decision_Method,AQT_sec,Simulation_Time_sec\n")

    for n in range(num_trials):
        for decision in decision_list:
            DECISION_MODE = decision
            start_time=time.time()

            for i in range(100000):
                #print("Step #{}".format(i))
                #print("observation is: ")
                #pprint(observation)
                #print("="*60)

                if DECISION_MODE == RANDOM:
                    node_to_sched = random.randint(0,7)
                    decision_name = "Random"
                elif DECISION_MODE == BBF:
                    node_to_sched, node = env.scheduler.get_best_bin_first_machine(dummy_job)
                    decision_name = "Best Bin First"
                elif DECISION_MODE == ORACLE:
                    env.scheduler.use_oracle = True
                    node_to_sched, node = env.scheduler.get_best_bin_first_machine(dummy_job)
                    decision_name = "Oracle"
                elif DECISION_MODE == SJN:
                    node_to_sched, node = env.scheduler.get_first_available_machine(dummy_job)
                    decision_name = "Shortest Job Next"
                elif DECISION_MODE == PPO or DECISION_MODE == PPO2:
                    if DECISION_MODE == PPO:
                        agent = agent_5k
                        decision_name = "PPO Agent 5k"
                    elif DECISION_MODE == PPO2:
                        agent = agent_3M
                        decision_name = "PPO Agent 3M"
                    node_probabilities = agent(torch.tensor(observation)).tolist()
                    ranked_nodes = []
                    i=0
                    for node in node_probabilities:
                        ranked_nodes.append( (i,node) )
                        i=i+1
                    ranked_nodes.sort(reverse=True,key=lambda x: x[1])
                    #node_to_sched = ranked_nodes[0][0]
                    for node in ranked_nodes:
                        if env.scheduler.cluster.machines[node[0]].can_schedule(dummy_job):
                            node_to_sched = node[0]
                            break
                        else:
                            num_agent_corrections = num_agent_corrections+1
                else:
                    print("Unknown decision mode")
                    break
                
                #rankings = env.scheduler.get_best_bin_first_machine_ranking(dummy_job)

                #print(f"Future jobs: {len(env.scheduler.future_jobs.queue)}")
                #print(f"Queued jobs: {len(env.scheduler.job_queue)}")
                #print(dummy_job)
                #print(env.scheduler.cluster.machines[node_to_sched])
                #print(f"{decision_name} decision is: {node_to_sched}")
                
                observation, reward, terminated, truncated, info = env.step(node_to_sched)

                # Toggle visualization by commenting this out
                #env.render()
            
                dummy_job = Job("job",
                                observation[0]*1000000,
                                observation[1],
                                observation[2],
                                observation[3]*3600,
                                0,
                                0)

                if terminated or truncated:
                    print(f"Terminated is {terminated}")
                    print(f"truncated is {truncated}")
                    break

            average_queue_time = env.scheduler.get_avg_job_queue_time()

            duration=round(time.time()-start_time,3)

            #print("Decision method: {}".format(decision_name))
            print("Trial {} for {} complete".format(n+1, decision_name))
            print("Average Queue Time: {} hours".format(round(average_queue_time/3600,2)))
            print("Simulation took {} seconds".format(duration))
            print("Num Agent corrections: {}".format(num_agent_corrections))

            if PPO in decision_list:
                csvfile.write(str(n+1) + "," + decision_name + "," + str(average_queue_time) + "," + str(duration) + "," + str(num_agent_corrections) +"\n")
            else:
                csvfile.write(str(n+1) + "," + decision_name + "," + str(average_queue_time) + "," + str(duration) + "\n")
            env.scheduler.reset(env.scheduler.machines_file, env.scheduler.jobs_file)
            observation = env.scheduler._get_obs()
            env.scheduler.use_oracle = False
            num_agent_corrections = 0
            dummy_job = Job("job",
                env.scheduler.job_queue[0].req_mem,
                env.scheduler.job_queue[0].req_cpus,
                env.scheduler.job_queue[0].req_gpus,
                env.scheduler.job_queue[0].req_duration,
                0,
                0)