
def test_ingest(raw_data_table, temp_butler_repo):
    """Ingest files into temp butler repo."""
    filenames = raw_data_table.query_column("filename")
    with temp_butler_repo:
        data_ids = temp_butler_repo.butler.queryMetadata('raw', ['visit'])
        assert len(data_ids) == 0
        temp_butler_repo.ingest_raw_data(filenames)
        data_ids = temp_butler_repo.butler.queryMetadata('raw', ['visit'])
        assert len(data_ids) == len(filenames)
