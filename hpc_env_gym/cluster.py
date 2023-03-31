from machine import Machine

class Cluster():
    def __init__(self, machine_list=[]):
        self.machines = machine_list
        
    def load_machines(self, csv_file_name):
        f = open(csv_file_name)
        lines = f.readlines()
        f.close()
        
        header = lines[0]
        lines = lines[1:]
        
        for line in lines:
            elements = line.split(",")
            self.machines.append(Machine(elements[0], *map(int, elements[1:])))
    
    def best_bin_first(self, job, global_clock):
        min_fill_margin = 10
        assigned_machine = None
        for m in self.machines:
            if (m.avail_mem >= job.req_mem) and (m.avail_cpus >= job.req_cpus) and (m.avail_gpus >= job.req_gpus):
                
                # not all nodes have both GPUs and CPUs, so init each margin to 0
                mem_margin = 0.0
                cpu_margin = 0.0
                gpu_margin = 0.0

                # count how many attributes the node has to normalize the final margin
                n_attributes = 0

                if m.total_mem > 0:
                    mem_margin = (m.avail_mem - job.req_mem)/m.total_mem
                    n_attributes += 1

                if m.total_cpus > 0:
                    cpu_margin = (m.avail_cpus - job.req_cpus)/m.total_cpus
                    n_attributes += 1

                if m.total_gpus > 0:
                    gpu_margin = (m.avail_gpus - job.req_gpus)/m.total_gpus
                    n_attributes += 1

                if n_attributes == 0:
                    print("{} has no virtual resources configured (all <= 0).".format(m.node_name))
                    fill_margin = 10
                else:
                    fill_margin = (mem_margin + cpu_margin + gpu_margin)/n_attributes

                if fill_margin < min_fill_margin:
                    min_fill_margin = fill_margin
                    assigned_machine = m
        
        if assigned_machine is not None:
            job.start_time = global_clock
            job.end_time = global_clock + job.actual_duration
            assigned_machine.start_job(job)
            return True, m.node_name
        else:
            return False, None

    def __str__(self):
        return "Cluster({})".format(self.machines)

    def __repr__(self):
        return "Cluster({})".format(self.machines)

    