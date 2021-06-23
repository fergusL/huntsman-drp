import os
import time
from threading import Thread

import numpy as np
import matplotlib.pyplot as plt

from panoptes.utils.time import CountdownTimer

from huntsman.drp.base import HuntsmanBase
from huntsman.drp.collection import RawExposureCollection, MasterCalibCollection


class Plotter(HuntsmanBase):
    """ Class to mass produce plots using data from Collection objects. """

    def __init__(self, find_kwargs=None, directory_prefix="default", plot_configs=None, **kwargs):
        """
        Args:
            find_kwargs (dict, optional): kwargs used to select documents to plot. Default: None.
            directory_prefix (str, optional): The subdirectory in the plot directory in which to
                store plots. Default: "default".
            plot_configs (dict, optional): Specify which keys to plot and how to plot them. For
                more info, see self.makeplots. Default: None.
            **kwargs: Parsed to HuntsmanBase init function.
        """
        super().__init__(**kwargs)

        self.image_dir = os.path.join(self.config["directories"]["plots"], directory_prefix)
        os.makedirs(self.image_dir, exist_ok=True)

        self._plot_configs = {} if not plot_configs else plot_configs

        self._exposure_collection = RawExposureCollection(config=self.config)
        self._calib_collection = MasterCalibCollection(config=self.config)

        find_kwargs = {} if find_kwargs is None else find_kwargs
        self._rawdocs = self._exposure_collection.find(**find_kwargs)
        self._caldocs = self._calib_collection.find(**find_kwargs)

    # Public methods

    def makeplots(self):
        """ Make all plots and write them to the images directory. """

        for d in self._plot_configs.get("plot_by_camera", []):
            self.plot_by_camera(**d)

        for d in self._plot_configs.get("plot_by_camera_filter", []):
            self.plot_by_camera_filter(**d)

        for d in self._plot_configs.get("hist_by_camera", []):
            self.plot_hist_by_camera(**d)

        for d in self._plot_configs.get("hist_by_camera_filter", []):
            self.plot_hist_by_camera_filter(**d)

    def plot_by_camera(self, x_key, y_key, basename=None, docs=None, linestyle=None,
                       marker="o", markersize=1, linewidth=0, **kwargs):
        """ x-y plot of quantities by camera.
        Args:
            x_key (str): Flattened name of the document field to plot on the x-axis.
            y_key (str): Flattened name of the document field to plot on the y-axis.
            basename (str, optional): The file basename. If not provided, key is used.
            docs (list of Document, optional): A list of documents to plot. If None, will use
                self._rawdocs.
            **kwargs: Parsed to matplotlib.pyplot.plot.
        """
        basename = basename if basename is not None else f"{x_key}_{y_key}"

        if docs is None:
            docs = self._rawdocs

        # Filter documents that have both data for x key and y key
        docs = [d for d in docs if (d.get(x_key) is not None) and (d.get(y_key) is not None)]

        docs_by_camera = self._get_docs_by_camera(docs)

        # Get dict of values organised by camera name
        x_values_by_camera, xmin, xmax = self._get_values_by_camera(x_key, docs_by_camera)
        y_values_by_camera, ymin, ymax = self._get_values_by_camera(y_key, docs_by_camera)

        if not any([_ for _ in x_values_by_camera.values()]):
            self.logger.warning(f"No {x_key} data to make plot for {basename}.")
            return
        if not any([_ for _ in y_values_by_camera.values()]):
            self.logger.warning(f"No {y_key} data to make plot for {basename}.")
            return

        # Make the plot
        fig, axes = self._make_fig_by_camera(n_cameras=len(x_values_by_camera))

        for (ax, cam_name) in zip(axes, x_values_by_camera.keys()):

            x_values = x_values_by_camera[cam_name]
            y_values = y_values_by_camera[cam_name]

            ax.plot(x_values, y_values, linestyle=linestyle, marker=marker, markersize=markersize,
                    linewidth=linewidth, **kwargs)

            ax.set_xlim(xmin, xmax)
            ax.set_ylim(ymin, ymax)
            ax.set_title(f"{cam_name}")

        fig.suptitle(basename)
        self._savefig(fig, basename=basename)

    def plot_by_camera_filter(self, x_key, y_key, **kwargs):
        """ x-y plot of quantities by camera for each filter.
        Args:
            x_key (str): Flattened name of the document field to plot on the x-axis.
            y_key (str): Flattened name of the document field to plot on the y-axis.
            **kwargs: Parsed to self.plot_by_camera.
        """
        filter_names = set([d["filter"] for d in self._rawdocs])

        for filter_name in filter_names:
            docs = [d for d in self._rawdocs if d["filter"] == filter_name]
            basename = f"{x_key}_{y_key}-{filter_name}"
            self.plot_by_camera(x_key, y_key, basename=basename, docs=docs, **kwargs)

    def plot_hist_by_camera(self, key, basename=None, docs=None, **kwargs):
        """ Plot histograms of quantities by camera.
        Args:
            key (str): Flattened name of the document field to plot.
            basename (str, optional): The file basename. If not provided, key is used.
            docs (list of Document, optional): A list of documents to plot. If None, will use
                self._rawdocs.
            **kwargs: Parsed to matplotlib.pyplot.
        """
        basename = basename if basename is not None else key

        if docs is None:
            docs = self._rawdocs
        docs_by_camera = self._get_docs_by_camera(docs)

        # Get dict of values organised by camera name
        values_by_camera, vmin, vmax = self._get_values_by_camera(key, docs_by_camera)

        if not any([_ for _ in values_by_camera.values()]):
            self.logger.warning(f"No {key} data to make hist for {basename}.")
            return

        # Make the plot
        fig, axes = self._make_fig_by_camera(n_cameras=len(values_by_camera))

        for (ax, (cam_name, values)) in zip(axes, values_by_camera.items()):
            ax.hist(values, range=(vmin, vmax), **kwargs)
            ax.set_title(f"{cam_name}")
        fig.suptitle(basename)

        self._savefig(fig, basename=basename)

    def plot_hist_by_camera_filter(self, key, **kwargs):
        """ Plot histograms of quantities by camera separately for each filter.
        Args:
            key (str): The flattened key to plot.
            **kwargs: Parsed to self.plot_hist_by_camera.
        """
        filter_names = set([d["filter"] for d in self._rawdocs])

        for filter_name in filter_names:
            docs = [d for d in self._rawdocs if d["filter"] == filter_name]
            basename = f"{key}-{filter_name}"
            self.plot_hist_by_camera(key, basename=basename, docs=docs, **kwargs)

    def _get_docs_by_camera(self, docs):
        """ Return dict of documents with keys of camera name.
        Args:
            docs (list): The list of docs.
        Returns:
            dict: Dict of camera_name: list of docs.
        """
        # Get camera names corresponding to CCD numbers
        cam_configs = self.config["cameras"]["devices"]
        # +1 because ccd numbering starts at 1
        camdict = {i + 1: cam_configs[i]["camera_name"] for i in range(len(cam_configs))}

        docs_by_camera = {}
        for ccd, cam_name in camdict.items():

            camera_docs = [d for d in docs if d["ccd"] == ccd]
            # Drop any cameras with no documents (e.g. testing cameras)
            if not camera_docs:
                self.logger.debug(f"No matching documents for camera {cam_name}.")
                continue

            docs_by_camera[cam_name] = camera_docs

        return docs_by_camera

    def _get_values_by_camera(self, key, docs_by_camera):
        """ Return dict of values with keys of camera name.
        Args:
            key (str): The name of the quantity to get.
            docs_by_camera (dict): Dict of cam_name: docs.
        Returns:
            dict: Dict of camera_name: list of values.
            float: The minimum value of all values
            flat: The maximum value of all values.
        """
        # Get dict of values organised by camera name
        values_by_camera = {}
        vmax = -np.inf
        vmin = np.inf
        for cam_name, docs in docs_by_camera.items():

            # Some measurements may be missing and get will return None
            values = [v for v in [d.get(key) for d in docs] if v is not None]
            values_by_camera[cam_name] = values

            # Update min / max for common range
            if values:
                vmin = min(np.nanmin(values), vmin)
                vmax = max(np.nanmax(values), vmax)

        return values_by_camera, vmin, vmax

    def _make_fig_by_camera(self, n_cameras, n_col=5, figsize=3):
        """ Make a figure with subplots for each camera.
        Args:
            n_cameras (int): The number of cameras.
            n_col (int, optional): The number of columns in the figure.
            figsize (int, optional): The size of each panel. Default: 3.
        Returns:
            matplotlib.pyplot.Figure: The figure object.
            list of matplotlib.pyplot.Axes: The axes for each subplot.
        """
        n_row = int((n_cameras - 1) / n_col) + 1
        fig = plt.figure(figsize=(n_col * figsize, n_row * figsize))

        axes = []
        for i in range(n_row):
            for j in range(n_col):
                idx = i * n_col + j
                if idx < n_cameras:
                    axes.append(fig.add_subplot(n_row, n_col, idx + 1))

        return fig, axes

    def _savefig(self, fig, basename, dpi=150, tight_layout=True):
        """ Save figure to images directory.
        Args:
            fig (matplotlib.pyplot.Figure): The figure to save.
            basename (str): The basename of the file to save the image to.
            tight_layout (bool, optional): If True (default), use tight layout for figure.
            **kwargs: Parsed to fig.savefig.
        """
        basename = basename.replace(".", "-") + ".png"  # Remove nested dict dot notation
        filename = os.path.join(self.image_dir, basename)
        self.logger.debug(f"Writing image: {filename}")

        if tight_layout:
            fig.tight_layout(rect=[0, 0.03, 1, 0.95])

        fig.savefig(filename, dpi=dpi, bbox_inches="tight")


class PlotterService(HuntsmanBase):
    """ Class to routinely update plots from multiple plotters specified in the config. """

    def __init__(self, sleep_interval=1, **kwargs):
        super().__init__(**kwargs)
        self.plotters = self._create_plotters()

        self._stop_threads = True
        self._sleep_interval = sleep_interval * 3600
        self._run_thread = Thread(target=self._run)

    @property
    def is_running(self):
        return self._run_thread.is_alive()

    def start(self):
        """ Start the service. """
        self.logger.debug(f"Starting {self}.")
        self._run_thread.start()

    def stop(self):
        """ Stop the service. """
        self.logger.debug(f"Stopping {self}.")
        self._stop_threads = True
        self._run_thread.join()
        self.logger.info(f"{self} stopped.")

    def _run(self):
        """ Continually update plots until the service is stopped. """
        self._stop_threads = False

        while True:
            for plotter in self.plotters:
                plotter.makeplots()

            self.logger.debug(f"Sleeping for {self._sleep_interval}s.")

            timer = CountdownTimer(self._sleep_interval)
            while not timer.expired():
                if self._stop_threads:
                    return
                time.sleep(1)

    def _create_plotters(self):
        """ Create a list of plotters from the config. """
        plotter_configs = self.config["plotter"]
        return [Plotter(config=self.config, logger=self.logger, **c) for c in plotter_configs]
