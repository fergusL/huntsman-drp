import matplotlib.pyplot as plt


def plot_wcs_box(document, ax, linestyle="-", color="k", linewidth=1, **kwargs):
    """ Plot the boundaries of the image in WCS coordinates.
    Args:
        documents (RawExposureDocument: The document to plot.
        ax (matplotlib.Axes): The axes instance.
        **kwargs: Parsed to matplotlib.pyplot.plot.
    """
    # Get the WCS
    wcs = document.get_wcs()

    # Get boundaries
    bl = wcs.pixel_to_world(0, 0)
    br = wcs.pixel_to_world(document["NAXIS1"], 0)
    tr = wcs.pixel_to_world(document["NAXIS1"], document["NAXIS2"])
    tl = wcs.pixel_to_world(0, document["NAXIS2"])

    plot_kwargs = dict(linestyle=linestyle, color=color, linewidth=linewidth)
    plot_kwargs.update(kwargs)

    # Plot box
    ax.plot([_.ra.to_value("deg") for _ in (bl, tl)],
            [_.dec.to_value("deg") for _ in (bl, tl)], **plot_kwargs)

    ax.plot([_.ra.to_value("deg") for _ in (tl, tr)],
            [_.dec.to_value("deg") for _ in (tl, tr)], **plot_kwargs)

    ax.plot([_.ra.to_value("deg") for _ in (tr, br)],
            [_.dec.to_value("deg") for _ in (tr, br)], **plot_kwargs)

    ax.plot([_.ra.to_value("deg") for _ in (br, bl)],
            [_.dec.to_value("deg") for _ in (br, bl)], **plot_kwargs)


def plot_wcs_boxes(documents, **kwargs):
    """ Plot the boundaries of the images in WCS coordinates.
    Args:
        documents (list of RawExposureDocument): The documents to plot.
        **kwargs: Parsed to matplotlib.pyplot.plot.
    Returns:
        matplotlib.Figure, matplotlib.Axes: The figure and axes.
    """
    fig, ax = plt.subplots()

    for document in documents:
        plot_wcs_box(document, ax, **kwargs)

    ax.set_xlabel("RA [deg]")
    ax.set_ylabel("Dec [deg]")

    return fig, ax
