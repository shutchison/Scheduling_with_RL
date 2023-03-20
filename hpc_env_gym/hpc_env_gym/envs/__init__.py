from gym.envs.registration import register

register(
    id='HPCEnv-v0',
    entry_point='hpc_env_gym.envs.hpc_env:HPCEnv'
)