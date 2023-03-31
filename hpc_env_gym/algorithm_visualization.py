import pyglet
import numpy as np
from matplotlib.figure import Figure
from matplotlib.backends.backend_agg import FigureCanvasAgg
import matplotlib.pyplot as plt
from scheduler import *

INCHES_PER_PIXEL = 1/plt.rcParams['figure.dpi']

class Algorithm_Visualization():
    # Going to make these manually set instead of auto-scaled to screen for ease of programming
    # All numbers in pixels except where noted
    PLOT_X_SIZE = 750
    PLOT_Y_SIZE = 680
    PLOT_Y_MARGIN = 20
    QUEUE_X_SIZE = 200
    MAX_NUM_IN_QUEUE = 30
    QUEUE_ITEM_HEIGHT = (PLOT_Y_SIZE)/(MAX_NUM_IN_QUEUE + 2) # add 2 for a title item and future jobs left item
    WINDOW_X_SIZE = PLOT_X_SIZE + QUEUE_X_SIZE - 20 # not sure where this extra 20px is coming from
    WINDOW_Y_SIZE = PLOT_Y_SIZE + PLOT_Y_MARGIN
    TITLE_BAR_SIZE = 60

    REQ_MEM_TERCILE_1 = 0
    REQ_MEM_TERCILE_2 = 0
    REQ_CPU_TERCILE_1 = 0
    REQ_CPU_TERCILE_2 = 0

    MAX_MACHINES = 12
    
    def __init__(self, all_jobs, machines) -> None:
        self.inspect_jobs(all_jobs)
        self.machines = machines
        self.window = pyglet.window.Window()
        self.pct_util = []
        self.tick = 0
        self.batch = pyglet.graphics.Batch()

    def inspect_jobs(self, jobs):
        req_cpu_list = []
        req_mem_list = []
        for i in range(len(jobs.queue)):
            job = jobs.queue[i][1]
            req_cpu_list.append(job.req_cpus)
            req_mem_list.append(job.req_mem)
        self.REQ_MEM_TERCILE_1 = np.quantile(req_mem_list,.33)
        self.REQ_MEM_TERCILE_2 = np.quantile(req_mem_list,.66)
        self.REQ_CPU_TERCILE_1 = np.quantile(req_cpu_list,.33)
        self.REQ_CPU_TERCILE_2 = np.quantile(req_cpu_list,.66)

    def run_visualizer(self, job_queue, jobs_left):
        self.set_window_size_loc()
        fig = self.create_figure()

        if len(self.machines) > self.MAX_MACHINES:
            data = self.package_machine_data_for_avg_plots()
            self.draw_avg_plots(fig)
        else:
            data = self.package_machine_data_for_bar_plots()
            self.draw_bar_plots(fig, data)
        
        graphs = self.render_figure(fig)
        
        # Don't remove these returned vars, batch library has aggressive garbage collection.
        # Returning from a function causes shape objects to be deleted unless they're stored somewhere
        job_queue_shapes, job_queue_labels = self.draw_job_queue(job_queue, jobs_left)

        @self.window.event
        def on_draw():
            self.window.clear()
            graphs.blit(0, 0)
            self.batch.draw()
            for label in job_queue_labels:
                label.draw()

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

    def draw_job_queue(self, job_queue, jobs_left):
        # =======================================================
        #    Sets some starter values for queue representation
        # =======================================================
        W = self.QUEUE_X_SIZE       # shape width
        H = self.QUEUE_ITEM_HEIGHT  # shape height
        X = self.PLOT_X_SIZE        # shape x position
        Y = self.PLOT_Y_SIZE        # shape y position

        shapes = []
        labels = []

        # there's probably a better way to do colors
        black  = (  0,   0,   0)
        white  = (255, 255, 255)

        # labels get positioned differently than shapes
        label_x_loc = X + self.QUEUE_X_SIZE/2
        
        # Check for more jobs than can be displayed
        overflow = True if len(job_queue) > self.MAX_NUM_IN_QUEUE else False
        print("# in viz job queue: {}".format(len(job_queue)))

        # =======================================================
        #              Creates the top title block
        # =======================================================
        title_label = pyglet.text.Label('Job Queue',
                                font_name='Times New Roman',
                                font_size=20,
                                x=label_x_loc,
                                y=Y,
                                anchor_x='center',
                                anchor_y='top',
                                color=(0,0,0,255)) # label colors are RGBA, last value is opacity

        labels.append(title_label)
        
        # Moves the vertical position down by one queue-shape-height's worth.
        # Labels get positioned by top edge position and shapes by bottom, so
        # increment the Y position appropriately
        Y = Y - H 

        # creates simple nested rectangles stored in a list for future rendering
        title_border = pyglet.shapes.Rectangle(X, Y, W, H, color=black, batch=self.batch)
        title_fill = pyglet.shapes.Rectangle(X+1, Y-1, W-2, H-2, color=white, batch=self.batch)

        shapes.append((title_border,title_fill))

        # ========================================================
        # Loop through the queue to create representative shapes
        # ========================================================
        job_ctr = 1
        for job in job_queue:
            Y = Y - H
            color = self.categorize_job_load(job)

            border = pyglet.shapes.Rectangle(X, Y, W, H, color=black, batch=self.batch)
            shape = pyglet.shapes.Rectangle(X+1, Y-1, W-2, H-2, color=color, batch=self.batch)

            gpu_tag = ""
            make_bold = False
            if job.req_gpus > 0:
                gpu_tag = "-gpu"
                make_bold = True

            label = pyglet.text.Label(job.job_name + gpu_tag,
                                    font_name='Times New Roman',
                                    font_size=12,
                                    bold=make_bold,
                                    x=label_x_loc,
                                    y=Y+H/2,
                                    anchor_x='center',
                                    anchor_y='center',
                                    color=(0,0,0,255)) # label colors are RGBA, last value is opacity

            shapes.append((shape,border))
            labels.append(label)

            job_ctr = job_ctr + 1
            
            # If there are too many items in the queue to display, use the last spot to make a note of that
            if overflow and job_ctr > self.MAX_NUM_IN_QUEUE-1: # -1 because we take up a valid spot to make this note
                Y = Y - H
                border = pyglet.shapes.Rectangle(X, Y, W, H, color=black, batch=self.batch)
                shape = pyglet.shapes.Rectangle(X+1, Y-1, W-2, H-2, color=(255,127,0), batch=self.batch) # orange color

                label = pyglet.text.Label("+{} more".format(len(job_queue)-(self.MAX_NUM_IN_QUEUE-1)),
                                        font_name='Times New Roman',
                                        font_size=12,
                                        x=label_x_loc,
                                        y=Y+H/2,
                                        anchor_x='center',
                                        anchor_y='center',
                                        color=(0,0,0,255)) # label colors are RGBA, last value is opacity

                shapes.append((shape,border))
                labels.append(label)
                break

        # ========================================================
        #       Adds an indicator for # future jobs remaining
        # ========================================================
        Y = 0
        future_label = pyglet.text.Label("# Future Jobs: {}".format(jobs_left),
                                font_name='Times New Roman',
                                font_size=12,
                                x=label_x_loc,
                                y=Y+H/2,
                                anchor_x='center',
                                anchor_y='center',
                                color=(0,0,0,255)) # label colors are RGBA, last value is opacity
        
        future_border = pyglet.shapes.Rectangle(X, Y, W, H, color=black, batch=self.batch)
        future_fill = pyglet.shapes.Rectangle(X+1, Y-1, W-2, H-2, color=white, batch=self.batch)

        shapes.append((future_fill,future_border))
        labels.append(future_label)

        return shapes, labels
    
    def categorize_job_load(self, job):
        # there's probably a better way to do colors
        red    = (255,   0,   0)
        yellow = (255, 255,   0)
        green  = (  0, 255,   0)

        count = 0

        if job.req_cpus >= self.REQ_CPU_TERCILE_1 and job.req_cpus < self.REQ_CPU_TERCILE_2:
            count = count + 1

        if job.req_mem >= self.REQ_MEM_TERCILE_1 and job.req_mem < self.REQ_MEM_TERCILE_2:
            count = count + 1

        if job.req_cpus >= self.REQ_CPU_TERCILE_2:
            count = count + 1

        if job.req_mem >= self.REQ_MEM_TERCILE_2:
            count = count + 1
        
        if count <= 1:
            color = green
        elif count == 2:
            color = yellow
        else:
            color = red

        return color


if (__name__ == '__main__'):
    s = Scheduler()
    s.load_cluster("more_machines.csv")
    s.load_jobs("jobs.csv")

    machines = s.cluster.machines
    jobs = s.future_jobs

    viz = Algorithm_Visualization(jobs, machines)

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
    for i in range(100):
        job = jobs.get(0)[1]
        if i >= 70:
            s.job_queue.append(job)

    viz.run_visualizer(s.job_queue,"Not Given")