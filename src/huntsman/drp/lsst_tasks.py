from lsst.pipe.tasks.ingest import IngestTask


def ingest_raw_data(filename_list, butler_directory, mode="link"):
    """

    """
    # Create the ingest task
    task = IngestTask()
    task = task.prepareTask(root=butler_directory, mode=mode)

    # Ingest the files
    task.ingestFiles(filename_list)
