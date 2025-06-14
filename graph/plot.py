import numpy as np

class LineGraphProperty:
    def __init__(self, label, marker, color, line_style):
        self.label = label
        self.marker = marker
        self.color = color
        self.line_style = line_style


class BarGraphProperty:
    def __init__(self, label, color, hatch):
        self.label = label
        self.color = color
        self.hatch = hatch


def export_legend(legend, filename, expand=[-5, -5, 5, 5]):
    fig = legend.figure
    fig.canvas.draw()
    bbox = legend.get_window_extent()
    bbox = bbox.from_extents(*(bbox.extents + np.array(expand)))
    bbox = bbox.transformed(fig.dpi_scale_trans.inverted())
    fig.savefig(filename, dpi="figure", bbox_inches=bbox)
