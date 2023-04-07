import gym
from pprint import pprint
from gym import spaces
from cluster import Cluster
from scheduler import Scheduler
from job import Job
from machine import Machine
import numpy as np
from algorithm_visualization import HPCEnvRenderer
from gym.spaces.utils import flatten_space
import numpy as np

MAX_MEM = 1548 # largest amount of memory in any one node in Gb
MAX_CPUs = 65 # largest number of CPUs in any one node
MAX_GPUs = 9 # largets number of GPUs in any one node
MAX_TIME = 673 # number of hours which is the longest job
NUM_MACHINES_IN_CLUSTER = 8 # how many machines are in the cluster?
MAX_RUNNING_JOBS = 100 # the most jobs on a machine at any time

# extending the OpenAI Gym environment
# https://www.gymlibrary.dev/api/core/

class HPCEnv(gym.Env):
    metadata = {"render_modes": ["human", "rgb_array"], "render_fps": 1}
    
    def __init__(self, render_mode=None):
        self.render_mode = render_mode
        #self.cluster = Cluster()
        self.scheduler = Scheduler()
        # Define the observation space as a dictionary which represents
        # the state of the job queue and the state of the machines in the HPC.
        # observation scace is the next job to be scheudled and all machines
        # next_job = [req_mem (in Gb), req_cpus, req_gpus, req_duration (in hours)]
        # state of all machines
        # machines = [[mem_avail (in Gb), cpus_avail, gpus_avail, num_running_jobs], [...], ...]
        # observation_space = [next_job, machines]

        next_job_tuple = spaces.Tuple(
            [spaces.Discrete(MAX_MEM),
            spaces.Discrete(MAX_CPUs),
            spaces.Discrete(MAX_GPUs),
            spaces.Discrete(MAX_TIME)
            ]
        )

        machine_tuple = spaces.Tuple(
            [spaces.Discrete(MAX_MEM),
            spaces.Discrete(MAX_CPUs),
            spaces.Discrete(MAX_GPUs),
            spaces.Discrete(MAX_RUNNING_JOBS) # need this to make it have the same number of elements that the job tuple has
            ]
        )

        self.observation_space = spaces.Tuple([next_job_tuple] + ([machine_tuple] * NUM_MACHINES_IN_CLUSTER))
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
        #job = self.scheduler.get_shortest_schedulable_job()
        #job_dict = {}
        #if job is not None:
        #    job_dict["job_name"] = job.job_name
        #    job_dict["req_mem"] = job.req_mem
        #    job_dict["req_cpus"] = job.req_cpus
        #    job_dict["req_gpus"] =  job.req_gpus
        #    job_dict["req_duration"] = job.req_duration
        #else:
        #    job_dict["job_name"] = "NoJob"
        #    job_dict["req_mem"] = 0
        #    job_dict["req_cpus"] = 0
        #    job_dict["req_gpus"] =  0
        #    job_dict["req_duration"] = 0
        
        next_job = self.scheduler.get_shortest_schedulable_job()
        if next_job != None:
            job_tuple = (next_job.req_mem//1000000, #convert to gb
                        next_job.req_cpus,
                        next_job.req_gpus,
                        next_job.req_duration//3600) #convert to hours
        else:
            job_tuple = (0, 0, 0, 0)
        #for testing    
        #job_list = []
        
        #job_queue_array = np.array(job_list)
        #print("Job queue array is:")
        #pprint(job_queue_array)
        #print("="*40)
        
        obs = [job_tuple]
        for machine in self.scheduler.cluster.machines:
            machine_status = (machine.avail_mem//1000000,
                              machine.avail_cpus,
                              machine.avail_gpus,
                              len(machine.running_jobs))
            obs.append(machine_status)

        obs_tuple = tuple(obs)
        #for testing
        #machine_list = []
        #machine_tuple = tuple(machine_list)
        #machine_array = np.array(machine_list)
        
        #for key, value in machine_array[0].items():
        #    print("{} = {} : {}".format(key, value, type(value)))
        #flat_machines = [item for sublist in machines_tuple for item in sublist]
        #combined_tuple = (job_tuple, flat_machines)

        #flat_list = [item for sublist in combined_tuple for item in sublist]

        #print(f"flat_list is {flat_list}")
        #return np.asarray(flat_list , dtype=np.int64 )
        #print("get_obs returns:")
        #pprint(obs_tuple)
        #print()
        return obs_tuple
    
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
        try:
            machines_csv_name = options["machines_csv"]
        except KeyError as e:
            raise KeyError("The reset method requires options[\"machines_csv\"] to be set to the csv containing the machines")
        except TypeError as e:
            raise KeyError("The reset method requires options[\"machines_csv\"] to be set to the csv containing the machines")
        try:
            jobs_csv_name = options["jobs_csv"]
        except KeyError as e:
            raise KeyError("The reset method requires options[\"jobs_csv\"] to be set to the csv containing the machines")
        except TypeError as e:
            raise KeyError("The reset method requires options[\"machines_csv\"] to be set to the csv containing the machines")
            
        self.scheduler.reset(machines_csv_name, jobs_csv_name)
        
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
        print("Action: {}".format(action))
        print("RL Agent attempting to schedule to {}".format(self.scheduler.cluster.machines[action].node_name))

        terminated = self.scheduler.tick()
        truncated = False
        reward = 0
        
        if not terminated:
            proposed_machine_index = action
            proposed_machine = self.scheduler.cluster.machines[proposed_machine_index]
            
            job = self.scheduler.get_shortest_schedulable_job()
            print("Trying to schedule {} on {}".format(job, proposed_machine))
            best_machine_index, best_machine = self.scheduler.get_best_bin_first_machine(job)
            
            if proposed_machine.can_schedule(job):
                self.scheduler.run_job(job, proposed_machine_index)
                if proposed_machine_index == best_machine_index:
                    reward = 1
                else:
                    reward = 0
            else:
                print("proposed machine {} lacks resources required to run {}".format(proposed_machine.node_name, job.job_name))
                self.scheduler.job_queue.append(job)
                reward = -1
                
        observation = self._get_obs()
        
        info = {}

        print(self.scheduler.summary_statistics())
        return observation, reward, terminated, truncated, info
        
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
    
        