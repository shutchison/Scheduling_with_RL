import pyglet
from pyglet import shapes

from matplotlib.figure import Figure
from matplotlib.backends.backend_agg import FigureCanvasAgg
import matplotlib.pyplot as plt
from scheduler import *

INCHES_PER_PIXEL = 1/plt.rcParams['figure.dpi']

class Algorithm_Visualization():
    # Going to make these manually set instead of auto-scaled to screen for ease of programming
    # All numbers in pixels except where noted
    PLOT_X_SIZE = 750
    PLOT_Y_SIZE = 980
    PLOT_Y_MARGIN = 20
    QUEUE_X_SIZE = 200
    WINDOW_X_SIZE = PLOT_X_SIZE + QUEUE_X_SIZE - 20 # not sure where this extra 20px is coming from
    WINDOW_Y_SIZE = PLOT_Y_SIZE + PLOT_Y_MARGIN
    TITLE_BAR_SIZE = 60

    MAX_MACHINES = 12
    
    def __init__(self, machines) -> None:
        self.machines = machines
        self.window = pyglet.window.Window()
        self.pct_util = []
        self.tick = 0
        self.max_cpus = -10
        self.max_mem = -10
        self.max_gpus = -10
        self.get_max_resources_avail(self.machines)
        self.batch = pyglet.graphics.Batch()

    def run_visualizer(self, job_queue):

        self.set_window_size_loc()
        fig = self.create_figure()

        if len(self.machines) > self.MAX_MACHINES:
            data = self.package_machine_data_for_avg_plots()
            self.draw_avg_plots(fig)
        else:
            data = self.package_machine_data_for_bar_plots()
            self.draw_bar_plots(fig, data)
        
        graphs = self.render_figure(fig)
        
        # don't delete, batch library has aggressive garbage collection
        # returning from a function causes shapes objects to be deleted they're unless stored somewhere
        job_queue_graphic = self.draw_job_queue(job_queue)

        @self.window.event
        def on_draw():
            self.window.clear()
            graphs.blit(0, 0)
            self.batch.draw()
            job_queue_graphic[0].draw()

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
            ax.set_ylim([0, 110])
            ax.bar_label(b, fmt="%0.1f%%")
            plot_num = plot_num + 1

    def draw_job_queue(self, job_queue):
        # X, Y, W, H

        W = self.QUEUE_X_SIZE
        H = 30
        X = self.PLOT_X_SIZE
        Y = self.PLOT_Y_SIZE

        job_shapes = []

        red = (255, 0, 0)
        yellow = (255, 255, 0)
        green = (0, 255, 0)
        black = (0, 0, 0)
        white = (255, 255, 255)

        label = pyglet.text.Label('Job Queue',
                          font_name='Times New Roman',
                          font_size=20,
                          x=X + self.QUEUE_X_SIZE/2, y=Y,
                          anchor_x='center', anchor_y='top', color=(0,0,0,255))

        job_shapes.append(label)

        Y = Y - H

        border = shapes.Rectangle(X, Y, W, H, color=white, batch=self.batch)
        shape = shapes.Rectangle(X+1, Y-1, W-2, H-2, color=white, batch=self.batch)

        job_shapes.append((shape,border))

        Y = Y - H

        for job in job_queue:
            estim = self.estimate_global_job_load(job)

            if estim < 8:
                color = green
            elif estim >= 8 and estim < 25:
                color = yellow
            else:
                color = red

            border = shapes.Rectangle(X, Y, W, H, color=black, batch=self.batch)
            shape = shapes.Rectangle(X+1, Y-1, W-2, H-2, color=color, batch=self.batch)

            Y = Y - H

            job_shapes.append((shape,border))

        return job_shapes

    def get_max_resources_avail(self, machines):
        for m in machines:
            if m.total_mem > self.max_mem:
                self.max_mem = m.total_mem
            if m.total_cpus > self.max_cpus:
                self.max_cpus = m.total_cpus
            if m.total_gpus > self.max_gpus:
                self.max_gpus = m.total_gpus
    
    def estimate_global_job_load(self, job):
        cpu_pct = job.req_cpus/self.max_cpus
        mem_pct = job.req_mem/self.max_mem
        gpu_pct = job.req_gpus/self.max_gpus
        return ((cpu_pct + mem_pct + gpu_pct) / 3)*100


if (__name__ == '__main__'):
    s = Scheduler()
    s.load_cluster("machines.csv")
    s.load_jobs("jobs.csv")

    machines = s.cluster.machines
    jobs = s.future_jobs

    viz = Algorithm_Visualization(machines)

    job = jobs.get(0)
    s.cluster.machines[0].start_job(job[1])
    job = jobs.get(0)
    s.cluster.machines[1].start_job(job[1])
    job = jobs.get(0)
    s.cluster.machines[2].start_job(job[1])
    job = jobs.get(0)
    s.cluster.machines[3].start_job(job[1])
    job = jobs.get(0)
    s.cluster.machines[4].start_job(job[1])
    job = jobs.get(0)
    s.cluster.machines[5].start_job(job[1])
    job = jobs.get(0)
    #s.cluster.machines[6].start_job(job[1])
    job = jobs.get(0)
    s.cluster.machines[7].start_job(job[1])
    for _ in range(5):
        job = jobs.get(0)[1]
        s.job_queue.append(job)
        print("Job {} System Load: {}%".format(job.job_name,viz.estimate_global_job_load(job)))

    viz.run_visualizer(s.job_queue)
    
    '''
    job = jobs.get(0)
    s.cluster.machines[2].start_job(job[1])
    viz = Algorithm_Visualization(machines)
    viz.run_visualizer()

    job = jobs.get(0)
    s.cluster.machines[2].start_job(job[1])
    viz = Algorithm_Visualization(machines)
    viz.run_visualizer()

    job = jobs.get(0)
    s.cluster.machines[3].start_job(job[1])
    viz = Algorithm_Visualization(machines)
    viz.run_visualizer()
    '''