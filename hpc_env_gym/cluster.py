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
    
    def __str__(self):
        return "Cluster({})".format(self.machines)

    def __repr__(self):
        return "Cluster({})".format(self.machines)

    