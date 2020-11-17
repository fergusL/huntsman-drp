import os
import json
import subprocess

from huntsman.drp.utils.date import parse_date


def test_parse_script(raw_data_table, config):
    """Call the script on a file and assert the printed metadata is correct."""
    # Get a raw data entry
    raw_data = raw_data_table.query().iloc[0]
    filename = raw_data["filename"]
    # Run the parsing scripy on the file
    cmd = f"python {os.environ['HUNTSMAN_DRP']}/nifi/parse_fits_metadata.py {filename}"
    output = subprocess.run(cmd, shell=True, stdout=subprocess.PIPE)
    # Convert output to dict
    metadata_json = output.stdout
    metadata = json.loads(metadata_json)
    # Make sure we have consistent values
    assert len(metadata) == len(raw_data) - 1   # No _id column
    for key, value in metadata.items():
        if key == config["mongodb"]["date_key"]:
            date_key = config["fits_header"]["date_key"]
            assert parse_date(value["$date"]) == parse_date(raw_data[date_key])
        else:
            assert raw_data[key] == value
