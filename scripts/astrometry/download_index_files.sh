#! /bin/bash
# Download astrometry index files.

# https://readthedocs.org/projects/astrometrynet/downloads/pdf/latest/

# The files are named like index-42XX.fits or index-42XX-YY.fits. XX is the “scale”, YY is the “healpix” number. These
# are called the “4200-series” index files.
# Each index file contains a large number of “skymarks” (landmarks for the sky) that allow our solver to identify your
# images. The skymarks contained in each index file have sizes (diameters) within a narrow range. You probably want
# to download index files whose quads are, say, 10% to 100% of the sizes of the images you want to solve.
# For example, let’s say you have some 1-degree square images. You should grab index files that contain skymarks of
# size 0.1 to 1 degree, or 6 to 60 arcminutes. Referring to the table below, you should grab index files 4203 through
# 4209. You might find that the same number of fields solve, and faster, using just one or two of the index files in the
# middle of that range - in our example you might try 4205, 4206 and 4207.

# Index Filename           Range of skymark diameters (arcminutes)
# index-4219.fits          1400–2000
# index-4218.fits          1000–1400
# index-4217.fits          680–1000
# index-4216.fits          480–680
# index-4215.fits          340–480
# index-4214.fits          240–340
# index-4213.fits          170–240
# index-4212.fits          120–170
# index-4211.fits          85–120
# index-4210.fits          60—85
# index-4209.fits          42–60
# index-4208.fits          30–42
# index-4207-*.fits        22–30
# index-4206-*.fits        16–22
# index-4205-*.fits        11–16
# index-4204-*.fits        8–11
# index-4203-*.fits        5.6–8.0
# index-4202-*.fits        4.0–5.6
# index-4201-*.fits        2.8–4.0
# index-4200-*.fits        2.0–2.8

# get index files for skymarks ranging 5.6 to 120 arcmins in diameter
mkdir -p ./index_data
wget -r -l1 -c -N --no-parent --no-host-directories --cut-dirs=2 -A "index-420[3-9]*.fits" -P ./index_data/ http://broiler.astrometry.net/~dstn/4200/
# because I don't know how to regex...``
wget -r -l1 -c -N --no-parent --no-host-directories --cut-dirs=2 -A "index-4211*.fits" -P ./index_data/ http://broiler.astrometry.net/~dstn/4200/
