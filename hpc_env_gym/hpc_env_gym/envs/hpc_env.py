import gym
from pprint import pprint
from gym import spaces
from cluster import Cluster
from scheduler import Scheduler
from job import Job
from machine import Machine
import numpy as np


MAX_MEM = 1024000001 # largest amount of memory in any one node
MAX_CPUs = 65 # largest number of CPUs in any one node
MAX_GPUs = 9 # largets number of GPUs in any one node
MAX_TIME = 31000000 # number of seconds which is the longest permitted job requested time (approx. one year)
NUM_MACHINES_IN_CLUSTER = 8 # how many machines are in the cluster?

# extending the OpenAI Gym environment
# https://www.gymlibrary.dev/api/core/

class HPCEnv(gym.Env):
    metadata = {"render_modes": ["human", "rgb_array"], "render_fps": 4}
    
    def __init__(self, render_mode=None):
        #self.cluster = Cluster()
        self.scheduler = Scheduler()
        # Define the observation space as a dictionary which represents
        # the state of the job queue and the state of the machines in the HPC.
        job_dict = {"job_name" : spaces.Text(42),
                    "req_mem" : spaces.Discrete(MAX_MEM),
                    "req_cpus" : spaces.Discrete(MAX_CPUs),
                    "req_gpus" : spaces.Discrete(MAX_GPUs),
                    "req_duration" : spaces.Discrete(MAX_TIME)}
        
        # for testing
        #job_dict = {}
                    
        job_dict_space = spaces.Dict(job_dict)
        job_queue_space = spaces.Sequence(job_dict_space)
        
        machine_dict = {"node_name" : spaces.Text(42),
                        #"percent_mem_avail" : spaces.Box(low=0.0, high=1.0),
                        "mem_avail" : spaces.Discrete(MAX_MEM),
                        #"percent_cpu_avail" : spaces.Box(low=0.0, high=1.0),
                        "cpu_avail" : spaces.Discrete(MAX_CPUs),
                        #"percent_gpu_avail" : spaces.Box(low=0.0, high=1.0),
                        "gpu_avail" : spaces.Discrete(MAX_GPUs)
                        }
                        
        
        #for testing        
        #machine_dict = {}
        
        machine_dict_space = spaces.Dict(machine_dict)
        machine_tuple = tuple([machine_dict_space] * NUM_MACHINES_IN_CLUSTER)
        machine_space = spaces.Tuple(machine_tuple)
        
        spaces_combined = {"job_queue" : job_queue_space,
                           "machines" : machine_space}
        
        self.observation_space = spaces.Dict(spaces_combined)
        
        self.action_space = spaces.Discrete(2)
    
    def node_num_to_name(self):
        pass
        
    def node_name_to_num(self):
        pass
    
    def _get_obs(self):
        job_list = []
        for job in self.scheduler.job_queue:
            d = {}
            d["job_name"] = job.job_name
            d["req_mem"] = job.req_mem
            d["req_cpus"] = job.req_cpus
            d["req_gpus"] =  job.req_gpus
            d["req_duration"] = job.req_duration
            job_list.append(d)
            
        #for testing    
        #job_list = []
        
        #job_queue_array = np.array(job_list)
        #print("Job queue array is:")
        #pprint(job_queue_array)
        print("="*40)
        
        machine_list = []
        for machine in self.scheduler.cluster.machines:
            d = {}
            d["node_name"] = machine.node_name
            #d["percent_mem_avail"] = machine.avail_mem/machine.total_mem
            d["mem_avail"] = machine.avail_mem
            #d["percent_cpu_avail"] = machine.avail_cpus/machine.total_cpus
            d["cpu_avail"] = machine.avail_cpus
            #if machine.avail_gpus == 0:
            #    d["percent_gpu_avail"] = 0.0
            #else:
            #    d["percent_gpu_avail"] = machine.avail_gpus/machine.total_gpus
            d["gpu_avail"] = machine.avail_gpus
            machine_list.append(d)
            
        #for testing
        #machine_list = []
        machine_tuple = tuple(machine_list)
        machine_array = np.array(machine_list)
        
        obs_dict = {"job_queue" : job_list,
                    "machines" : machine_tuple}
        #for key, value in machine_array[0].items():
        #    print("{} = {} : {}".format(key, value, type(value)))
        return obs_dict

    def reset(self, seed=None, options=None):
        """
        PARAMETERS:
        seed (optional int) – The seed that is used to initialize the environment’s PRNG.
        If the environment does not already have a PRNG and seed=None (the default option)
        is passed, a seed will be chosen from some source of entropy (e.g. timestamp or /dev/urandom).
        However, if the environment already has a PRNG and seed=None is passed, the PRNG will not be reset.
        If you pass an integer, the PRNG will be reset even if it already exists. Usually, you want to pass
        an integer right after the environment has been initialized and then never again. Please refer to the
        minimal example above to see this paradigm in action.

        options (optional dict) – Additional information to specify how the environment is reset (optional,
        depending on the specific environment)

        RETURNS:
        observation (object) – Observation of the initial state. This will be an element of observation_space
        (typically a numpy array) and is analogous to the observation returned by step().

        info (dictionary) – This dictionary contains auxiliary information complementing observation.
        It should be analogous to the info returned by step().
        """
        try:
            machines_csv_name = options["machines_csv"]
        except KeyError as e:
            raise KeyError("The reset method requires options[\"machines_csv\"] to be set to the csv containing the machines")
        try:
            jobs_csv_name = options["jobs_csv"]
        except KeyError as e:
            raise KeyError("The reset method requires options[\"jobs_csv\"] to be set to the csv containing the machines")
            
        self.scheduler.reset(machines_csv_name, jobs_csv_name)
        
        obs = self._get_obs()
        #print("obs is of type {}:".format(type(obs)))
        #pprint(obs)
        info = {}
        
        return obs, info
    
    def step(self, action):
        """
        observation (object) – this will be an element of the environment’s observation_space.
        This may, for instance, be a numpy array containing the positions and velocities of certain objects.

        reward (float) – The amount of reward returned as a result of taking the action.

        terminated (bool) – whether a terminal state (as defined under the MDP of the task) is reached.
        In this case further step() calls could return undefined results.

        truncated (bool) – whether a truncation condition outside the scope of the MDP is satisfied.
        Typically a timelimit, but could also be used to indicate agent physically going out of bounds. Can be used to end the episode prematurely before a terminal state is reached.

        info (dictionary) – info contains auxiliary diagnostic information
        (helpful for debugging, learning, and logging). This might, for instance,
        contain: metrics that describe the agent’s performance state, variables that are
        hidden from observations, or individual reward terms that are combined to produce
        the total reward. It also can contain information that distinguishes truncation and termination,
        however this is deprecated in favour of returning two booleans, and will be removed in a future version.


        done (bool) – A boolean value for if the episode has ended, in which case further step() calls will return undefined results. A done signal may be emitted for different reasons: Maybe the task underlying the environment was solved successfully, a certain timelimit was exceeded, or the physics simulation has entered an invalid state.
        """
        if action == 1:
            job = self.scheduler.job_queue.pop(0)
            print(job)
            self.scheduler.cluster.machines[0].start_job(job)
        
        observation = self._get_obs()
        reward = 1
        terminated = False
        info = {}

        return observation, reward, terminated, False, info
        
    def close(self):
        #check if renderer is "human" and destroy window if it is
        pass
    
    def render(self):
        pass
    
        