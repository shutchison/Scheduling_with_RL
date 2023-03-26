import pyglet

from matplotlib.figure import Figure
from matplotlib.backends.backend_agg import FigureCanvasAgg
import matplotlib.pyplot as plt
import numpy as np

INCHES_PER_PIXEL = 1/plt.rcParams['figure.dpi']

class HPCSchedAlgoViz():
    # Going to make these manually set instead of auto-scaled to screen for ease of programming
    # All numbers in pixels except where noted
    PLOT_X_SIZE = 750
    PLOT_Y_SIZE = 980
    WINDOW_X_SIZE = PLOT_X_SIZE + 200
    WINDOW_Y_SIZE = 1000
    TITLE_BAR_SIZE = 60
    

    window = pyglet.window.Window()

    def main(self):

        self.set_window_size_loc()
        
        Y1 = [10,11,12]
        Y2 = [4,5,6]
        Y = Y1,Y2,Y1,Y2,Y1,Y2,Y1,Y2,Y1,Y2,Y1,Y2
        
        dpi_res = min(self.window.get_size()[0], self.window.get_size()[1]) / 10

        fig = Figure((self.window.get_size()[0] / dpi_res, self.window.get_size()[1] / dpi_res))
        self.draw_bar_plots(fig,Y)
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


    def render_figure(self, fig):
        canvas = FigureCanvasAgg(fig)
        data, (w, h) = canvas.print_to_buffer()
        return pyglet.image.ImageData(w, h, "RGBA", data, -4 * w)

    def draw_bar_plots(self, fig, Y):
        plot_num = 1
        for y in Y:
            ax = fig.add_subplot(4,3,plot_num)
            fig.set_size_inches(self.PLOT_X_SIZE*INCHES_PER_PIXEL, self.PLOT_Y_SIZE*INCHES_PER_PIXEL)
            fig.tight_layout()
            labels=["CPUs","Mem","GPUs"]
            b = ax.bar(labels, y)
            ax.set(ylabel='% Utilization', title='Machine')
            ax.bar_label(b)
            plot_num = plot_num + 1

if (__name__ == '__main__'):
    viz = HPCSchedAlgoViz()
    viz.main()