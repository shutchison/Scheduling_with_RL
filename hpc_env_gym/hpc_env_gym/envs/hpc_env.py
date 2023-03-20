import gym
from gym import spaces
from cluster import Cluster
from job import Job
from machine import Machine

# extending the OpenAI Gym environment
# https://www.gymlibrary.dev/api/core/

class HPCEnv(gym.Env):
    metadata = {"render_modes": ["human", "rgb_array"], "render_fps": 4}
    
    def __init__(self, render_mode=None):
        
        self.cluster = Cluster()
        
        self.action_space = spaces.Discrete(2)
        self.observation_space = spaces.Discrete(2)

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
        obs = 1
        info = {}
        
        print("in reset method")
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
        observation = ""
        reward = 1
        terminated = False
        info = {}

        return observation, reward, terminated, False, info
        
    def close(self):
        #check if renderer is "human" and destroy window if it is
        pass
    
    def render(self):
        pass
    
        