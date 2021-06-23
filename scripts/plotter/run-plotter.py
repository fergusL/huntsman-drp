import os
from huntsman.drp.services.plotter import PlotterService

if __name__ == "__main__":

    plotter = PlotterService()

    # Set niceness level
    niceness = plotter.config.get("niceness", None)
    if niceness:
        os.nice(niceness - os.nice(0))

    plotter.start()
