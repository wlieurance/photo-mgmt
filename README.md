# photo_mgmt
Tools used for photo management.

# Installation
USers will need [Python 3](https://www.python.org/) for the python script and [R](https://www.r-project.org/) for the R script
For the python script please use pip and the distributed requirements.txt file (pip install -r requirements.txt). You will also need to install the [spatialite](https://www.gaia-gis.it/fossil/libspatialite/index) library *(mod_spatialite)* and add it to your system path if it is not already present.

For the R scripts, the required libraries are at the beginning of the R script(s), and users will have to check and install.packages manually. No spatialite is required for the R scripts currently.

# Usage 
Script calls are made via *python* or *Rscript*.  Use the '-h' argument to get list of arguments and options (e.g. *Rscript ./PhotoMetadata.R -h* or *python ./PhotoMetadata.py -h* ).
Both python and R scripts will recursively scan a directory for photos, calculate their md5hash and extract their EXIF tags for storage in a SQLite database.  The python script currently assumes that the spatialite library is loadable, though in the future this will likely be made optional.

The python script stores the EXIF tags in a long format while the R script stores them in a wide format, though essentially both scripts do the same thing.

# Contributing
If you want to add functionality or error checking features to anything here, please feel free to contact the author.

# Credits 
Author: Wade Lieurance

# License 
Gnu Public License v3