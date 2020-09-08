![Python Tests](https://github.com/AstroHuntsman/huntsman-drp/workflows/Python%20Tests/badge.svg?branch=develop)
![Docker CI](https://github.com/AstroHuntsman/huntsman-drp/workflows/Docker%20CI/badge.svg)

# huntsman-drp
The Huntsman data reduction pipeline (`huntsman-drp`) is responsible for creating calibrated science data from raw images taken by the Huntsman telephoto array. The pipeline uses the LSST code stack configured using the [AstroHuntsman/obs_huntsman](https://github.com/AstroHuntsman/obs_huntsman) package.

## Testing
To run tests locally, ensure that the `HUNTSMAN_DRP` and `OBS_HUNTSMAN` environment variables point to the `huntsman-drp` and `obs_huntsman` repositories respectively. Testing is done inside a docker container:
```
cd $HUNTSMAN_DRP/docker/testing
docker-compose up
```
When the tests have finished, you might need to ``ctrl+c`` cancel the test script. 

When finished testing, be sure to type the following to shut down the docker containers:
```
docker-compose down
```

You can view an html coverage report after the tests complete using the following on OSX:
```
open ../../src/huntsman/drp/htmlcov/index.html
```
