from tempfile import NamedTemporaryFile

from huntsman.drp.butler import ButlerRepository
from huntsman.drp.refcat import TapReferenceCatalogue


if __name__ == "__main__":

    ra_list = [329.4195, 334.01979167]
    dec_list = [-80.358, -7.89822222]
    radius_degrees = 2

    butler_repository = ButlerRepository("/opt/lsst/software/stack/DATA")

    with NamedTemporaryFile() as tf:
        filename = tf.name

        # Make a refcat in .csv format
        rc = TapReferenceCatalogue()
        rc.create_refcat(ra_list, dec_list, filename=filename, radius_degrees=radius_degrees)

        # Convert to LSST format and ingest
        butler_repository.ingest_reference_catalogue(filenames=[filename])
