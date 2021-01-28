"""
Make vignetting test plot for flat fields.
"""
from multiprocessing import Pool
import matplotlib.pyplot as plt
import numpy as np
from astropy.io import fits

from huntsman.drp.datatable import ExposureTable
from huntsman.drp.quality.vignetting import calculate_asymmetry_statistics

INTERVAL = 7
OUTPUT_FILENAME = "vignetting_flats.png"
NPROC = 8


def calculate_asymmetry(filename):
    """Load data and calculate statistics."""
    data = fits.getdata(filename).astype("float")
    return calculate_asymmetry_statistics(data)


if __name__ == "__main__":

    # Move these to script args
    interval_days = INTERVAL
    output_filename = OUTPUT_FILENAME

    # Get recent flat field images
    datatable = ExposureTable()
    # This is a hack to cope with the non-standard field naming
    metalist = datatable.query_latest(days=interval_days, dataType="science")
    filenames = []
    ccd_names = []
    for m in metalist:
        if m["FIELD"].startswith("Flat"):
            filenames.append(m["filename"])
            ccd_names.append(m["INSTRUME"])

    # Calculate asymmetry statistics
    print(f"Processing {len(filenames)} flat fields.")
    with Pool(NPROC) as pool:
        results = pool.map(calculate_asymmetry, filenames)

    plt.figure()
    for ccd_name in np.unique(ccd_names):
        x = [r[0] for c, r in zip(ccd_names, results) if c == ccd_name]
        y = [r[1] for c, r in zip(ccd_names, results) if c == ccd_name]
        plt.plot(x, y, marker='o', markersize=1.75, linestyle=None, label=ccd_name, linewidth=0)
    plt.grid()
    plt.xlabel("Horizontal Asymmetry [ADU]")
    plt.ylabel("Vertical Asymmetry [ADU]")
    plt.legend(loc="best", fontsize=9)
    plt.savefig(OUTPUT_FILENAME, bbox_inches="tight", dpi=150)
