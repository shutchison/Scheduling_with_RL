import pyglet

from matplotlib.figure import Figure
from matplotlib.backends.backend_agg import FigureCanvasAgg
import matplotlib.pyplot as plt
import numpy as np
from scheduler import *

INCHES_PER_PIXEL = 1/plt.rcParams['figure.dpi']

class Algorithm_Visualization():
    # Going to make these manually set instead of auto-scaled to screen for ease of programming
    # All numbers in pixels except where noted
    PLOT_X_SIZE = 750
    PLOT_Y_SIZE = 980
    WINDOW_X_SIZE = PLOT_X_SIZE + 200
    WINDOW_Y_SIZE = 1000
    TITLE_BAR_SIZE = 60

    MAX_MACHINES = 12
    
    def __init__(self, machines) -> None:
        self.machines = machines
        self.window = pyglet.window.Window()
        self.pct_util = []
        self.tick = 0

    def run_visualizer(self):
        self.set_window_size_loc()
        fig = self.create_figure()

        if len(self.machines) > self.MAX_MACHINES:
            data = self.package_machine_data_for_avg_plots()
            self.draw_avg_plots(fig)
        else:
            data = self.package_machine_data_for_bar_plots()
            self.draw_bar_plots(fig, data)
        
        graphs = self.render_figure(fig)
        
        @self.window.event
        def on_draw():
            self.window.clear()
            graphs.blit(0, 0)

        pyglet.app.run()

    def set_window_size_loc(self):
        display = pyglet.canvas.Display()
        screen = display.get_default_screen()

        x_size = min(int(self.WINDOW_X_SIZE), screen.width)
        y_size = min(int(self.WINDOW_Y_SIZE), screen.height) - self.TITLE_BAR_SIZE

        x_loc = int((screen.width - x_size) / 2)
        y_loc = int((screen.height - y_size) / 2)

        self.window.set_size(x_size, y_size)
        self.window.set_location(x_loc, y_loc)

    def create_figure(self):
        dpi_res = min(self.window.get_size()[0], self.window.get_size()[1]) / 10
        return Figure((self.window.get_size()[0] / dpi_res, self.window.get_size()[1] / dpi_res))

    def render_figure(self, fig):
        canvas = FigureCanvasAgg(fig)
        data, (w, h) = canvas.print_to_buffer()
        return pyglet.image.ImageData(w, h, "RGBA", data, -4 * w)

    def package_machine_data_for_avg_plots(self):
        n_machines = len(machines)
        util_sum = 0
        for m in self.machines:
            util_sum = util_sum + (m.mem_util_pct + m.cpus_util_pct + m.gpus_util_pct)/3
        avg_utilization = util_sum/n_machines
        self.pct_util.append(avg_utilization)
        self.tick = self.tick + 1

    def draw_avg_plots(self, fig):
        ax = fig.add_subplot(111)
        fig.set_size_inches(self.PLOT_X_SIZE*INCHES_PER_PIXEL, self.PLOT_Y_SIZE*INCHES_PER_PIXEL)
        fig.tight_layout()
        ax.plot(self.pct_util)
        ax.set(ylabel='%', title="Average Node % Utilization")

    def package_machine_data_for_bar_plots(self):
        m_data = []
        for m in self.machines:
            m_data.append((m.node_name, (m.cpus_util_pct, m.mem_util_pct, m.gpus_util_pct)))
        return m_data

    def draw_bar_plots(self, fig, data_sets):
        plot_num = 1
        for i in range(len(data_sets)):
            machine_name = data_sets[i][0]
            machine_data = data_sets[i][1]
            ax = fig.add_subplot(4,3,plot_num)
            fig.set_size_inches(self.PLOT_X_SIZE*INCHES_PER_PIXEL, self.PLOT_Y_SIZE*INCHES_PER_PIXEL)
            fig.tight_layout()
            labels=["CPUs","Mem","GPUs"]
            b = ax.bar(labels, machine_data)
            ax.set(ylabel='% Utilization', title=machine_name)
            ax.set_ylim([0, 100])
            ax.bar_label(b)
            plot_num = plot_num + 1

if (__name__ == '__main__'):
    sched = Scheduler()
    sched.load_cluster("machines.csv")
    machines = sched.cluster.machines
    viz = Algorithm_Visualization(machines)
    viz.run_visualizer()
    #sched.conduct_simulation("machines.csv","jobs.csv")