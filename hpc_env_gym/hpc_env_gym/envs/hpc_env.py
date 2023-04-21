import gym
from pprint import pprint
from gym import spaces
from cluster import Cluster
from scheduler import Scheduler
from job import Job
from machine import Machine
import numpy as np
from algorithm_visualization import HPCEnvRenderer
import numpy as np
from copy import deepcopy

MAX_MEM = 1548 # largest amount of memory in any one node in Gb
MAX_CPUs = 65 # largest number of CPUs in any one node
MAX_GPUs = 9 # largets number of GPUs in any one node
MAX_TIME = 673 # number of hours which is the longest job
NUM_MACHINES_IN_CLUSTER = 8 # how many machines are in the cluster?
MAX_RUNNING_JOBS = 100 # the most jobs on a machine at any time

step_counter = 0
steps_between_prints = 10000
# Unsure how to pass these in, so hard coding for the moment
machines_csv_name = "more_machines.csv"
jobs_csv_name = "5000_jobs.csv"

# extending the OpenAI Gym environment
# https://www.gymlibrary.dev/api/core/

class HPCEnv(gym.Env):
    metadata = {"render_modes": ["human", "rgb_array"], "render_fps": 1}
    
    def __init__(self, render_mode=None):
        print(f"using {machines_csv_name} and {jobs_csv_name}")
        self.render_mode = render_mode
        #self.cluster = Cluster()
        self.scheduler = Scheduler()

        self.scheduler.reset(machines_csv_name, jobs_csv_name)
        
        self.get_machine_parameters()
        self.get_job_parameters()

        # Define the observation space as a dictionary which represents
        # the state of the job queue and the state of the machines in the HPC.
        # observation scace is the next job to be scheudled and all machines

        # next_job = [req_mem (in Gb), req_cpus, req_gpus, req_duration (in hours)]
        # state of all machines
        # machines = [[mem_avail (in Gb), cpus_avail, gpus_avail, num_running_jobs], [...], ...]
        # observation_space = [next_job, machines]

        # self.observations space needs to be as flat as possible for ppo?
        # next_job details in the first four places
        obs = [MAX_MEM, MAX_CPUs, MAX_GPUs, MAX_TIME]
        # Status for each machine in the cluster
        obs.extend([MAX_MEM, MAX_CPUs, MAX_GPUs] * NUM_MACHINES_IN_CLUSTER)

        low = np.array([0]* (4 + 3*NUM_MACHINES_IN_CLUSTER))
        high = np.array(obs)

        self.observation_space = spaces.Box(low, high)

        #print()
        #print("self.observation_space is ")
        #pprint(self.observation_space)
        #print()

        self.action_space = spaces.Discrete(NUM_MACHINES_IN_CLUSTER)
        
        self.job_assignments = []
        self.renderer = None
        
    def node_num_to_name(self):
        pass
        
    def node_name_to_num(self):
        pass
    
    def _get_obs(self):
        # I'm pretty sure this way works just as well the one right after it, but we have the system
        # working now and I don't want to touch it
        #machines = []
        #machines = [[machine.avail_mem//1000000,
        #             machine.avail_cpus,
        #             machine.avail_gpus] for machine in self.scheduler.cluster.machines]
        #flat_machines = [item for sublist in machines for item in sublist]
        
        flat_machines = []
        for machine in self.scheduler.cluster.machines:
            flat_machines.append(machine.avail_mem//1000000)
            flat_machines.append(machine.avail_cpus)
            flat_machines.append(machine.avail_gpus)

        next_job = self.scheduler.peek_shortest_schedulable_job()
        job = []
        if next_job is not None:
            job = [next_job.req_mem//1000000, #convert to gb
                    next_job.req_cpus,
                    next_job.req_gpus,
                    next_job.req_duration//3600 #convert to hours
                    ]
        else:
            job = [0,0,0,0]
        
        obs = job + flat_machines

        obs_array = np.array(obs, dtype=np.float32)
        
        #print(f"_get_obs machines: {flat_machines}")
        #print(f"_get_obs job: {job}")
        #print(f"_get_obs is returning: {obs_array}\n{obs_array.shape}")

        return obs_array
    
    def get_machine_parameters(self):
        global MAX_MEM, MAX_CPUs, MAX_GPUs, NUM_MACHINES_IN_CLUSTER, MAX_RUNNING_JOBS
        
        mems = []
        cpus = []
        gpus = []
        for machine in self.scheduler.cluster.machines:
            mems.append(machine.total_mem//1000000)
            cpus.append(machine.total_cpus)
            gpus.append(machine.total_gpus)
        
        MAX_MEM = max(mems)
        MAX_CPUs = max(cpus)
        MAX_GPUs = max(gpus)
        NUM_MACHINES_IN_CLUSTER = len(self.scheduler.cluster.machines)
        MAX_RUNNING_JOBS = 100 # not sure what to set this to    

    def get_job_parameters(self):
        global MAX_TIME
        
        req_t = []
        for job in self.scheduler.future_jobs.queue:
            req_t.append(job[1].req_duration//3600)
        
        MAX_TIME = max(req_t)

    def reset(self, seed=None, options=None):
        """
        PARAMETERS:
        seed (optional int) - The seed that is used to initialize the environment's PRNG.
        If the environment does not already have a PRNG and seed=None (the default option)
        is passed, a seed will be chosen from some source of entropy (e.g. timestamp or /dev/urandom).
        However, if the environment already has a PRNG and seed=None is passed, the PRNG will not be reset.
        If you pass an integer, the PRNG will be reset even if it already exists. Usually, you want to pass
        an integer right after the environment has been initialized and then never again. Please refer to the
        minimal example above to see this paradigm in action.

        options (optional dict) - Additional information to specify how the environment is reset (optional,
        depending on the specific environment)

        RETURNS:
        observation (object) - Observation of the initial state. This will be an element of observation_space
        (typically a numpy array) and is analogous to the observation returned by step().

        info (dictionary) - This dictionary contains auxiliary information complementing observation.
        It should be analogous to the info returned by step().
        """

        self.scheduler.reset(machines_csv_name, jobs_csv_name)
        
        self.get_machine_parameters()
        self.get_job_parameters()

        if self.render_mode == "human":
            arg_dict = {}
            arg_dict["all_jobs"] = self.scheduler.future_jobs
            arg_dict["all_machines"] = self.scheduler.cluster.machines
            self.renderer = HPCEnvRenderer(self.render_mode, arg_dict)
        
        obs = self._get_obs()
        #print("obs is of type {}:".format(type(obs)))
        #pprint(obs)
        info = {}
        #self.job_assignments = []

        #print("reset returning:")
        pprint((obs, info))

        return obs, info
    
    def step(self, action):
        """
        observation (object) - this will be an element of the environment's observation_space.
        This may, for instance, be a numpy array containing the positions and velocities of certain objects.

        reward (float) - The amount of reward returned as a result of taking the action.

        terminated (bool) - whether a terminal state (as defined under the MDP of the task) is reached.
        In this case further step() calls could return undefined results.

        truncated (bool) - whether a truncation condition outside the scope of the MDP is satisfied.
        Typically a timelimit, but could also be used to indicate agent physically going out of bounds. Can be used to end the episode prematurely before a terminal state is reached.

        info (dictionary) - info contains auxiliary diagnostic information
        (helpful for debugging, learning, and logging). This might, for instance,
        contain: metrics that describe the agent's performance state, variables that are
        hidden from observations, or individual reward terms that are combined to produce
        the total reward. It also can contain information that distinguishes truncation and termination,
        however this is deprecated in favour of returning two booleans, and will be removed in a future version.


        done (bool) - A boolean value for if the episode has ended, in which case further step() calls will return undefined results. A done signal may be emitted for different reasons: Maybe the task underlying the environment was solved successfully, a certain timelimit was exceeded, or the physics simulation has entered an invalid state.
        """
        
        # The action is the index of the machine on which to start this job
        #print("Action: {}".format(action))
        # print("RL Agent attempting to schedule to {}".format(self.scheduler.cluster.machines[action].node_name))

        truncated = False
        terminated = self.scheduler.simulation_is_complete()
        reward = 0
        
        if not terminated:
            proposed_machine_index = action
            proposed_machine = self.scheduler.cluster.machines[proposed_machine_index]
            
            job = self.scheduler.pop_shortest_schedulable_job()
            if job is not None:
                #print("Trying to schedule {} on {}".format(job, proposed_machine))
                best_machine_index, best_machine = self.scheduler.get_best_bin_first_machine(job)
                
                #reward = self.integer_reward(proposed_machine, proposed_machine_index, best_machine_index, job)
                reward = self.ranked_reward(job, proposed_machine_index)
            
            terminated = self.scheduler.tick()

        observation = self._get_obs()
        info = {} 

        if terminated or truncated:
            metric = round(self.scheduler.get_avg_job_queue_time(),2)
            print(f"Avg job queue time for this run was {metric:,}")
        #populate with episode and avg job queue time when simulation terminates
        # if terminated or truncated:
        #     info["episode"] = {}
        #     info["episode"]["r"] = self.scheduler.get_avg_job_queue_time()
        #     info["episode"]["l"] = self.scheduler.global_clock - self.scheduler.clock_offset
        # print(f"Rewarded with {reward}")
        # print(self.scheduler.summary_statistics())
        global step_counter
        step_counter += 1
        if step_counter % steps_between_prints == 0:
            print(f"Step {step_counter}:")
            print(self.scheduler.summary_statistics())
        return observation, reward, terminated, truncated, info

    def integer_reward(self, proposed_machine, proposed_machine_index, best_machine_index, job):
        if proposed_machine.can_schedule(job):
            self.scheduler.run_job(job, proposed_machine_index)
            if proposed_machine_index == best_machine_index:
                reward = 2
            else:
                reward = 1
        else:
            #print("proposed machine {} lacks resources required to run {}".format(proposed_machine.node_name, job.job_name))
            self.scheduler.job_queue.append(job)
            reward = 0
        return reward

    def ranked_reward(self, job, proposed_machine_index):
        rankings = self.scheduler.get_best_bin_first_machine_ranking(job)
        current_margin = rankings[0][1]
        reward = len(rankings)
        for rank in rankings:
            if rank[1] != current_margin:
                reward = reward - 1
                current_margin = rank[1]
            if proposed_machine_index == rank[0]:
                if rank[1] == -1:
                    reward = -1
                break
        if reward < 0:
            #print("proposed machine {} lacks resources required to run {}".format(proposed_machine.node_name, job.job_name))
            self.scheduler.job_queue.append(job)

        return reward

    def close(self):
        #check if renderer is "human" and destroy window if it is
        pass
    
    def render(self):
        if self.renderer == None:
            arg_dict = {}
            arg_dict["all_jobs"] = self.scheduler.future_jobs
            arg_dict["all_machines"] = self.scheduler.cluster.machines
            self.renderer = HPCEnvRenderer(self.render_mode, arg_dict)
            
        state = [self.scheduler.job_queue, len(self.scheduler.future_jobs.queue), self.scheduler.global_clock, False]
        rendered_thing = self.renderer.render(state)
        
        return rendered_thing
    
        
