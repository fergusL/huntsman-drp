import os
import pytest


def test_ingest(metadatabase, temp_butler_repo):
    """Ingest files into temp butler repo."""
    filenames = metadatabase.query_files()
    with temp_butler_repo:
        data_ids = temp_butler_repo.butler.queryMetadata('raw', ['visit'])
        assert len(data_ids) == 0
        temp_butler_repo.ingest_raw_data(filenames)
        data_ids = temp_butler_repo.butler.queryMetadata('raw', ['visit'])
        assert len(data_ids) == len(filenames)
