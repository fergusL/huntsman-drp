from multiprocessing import Process

from huntsman.drp.utils.pyro import NameServer, wait_for_nameserver
from huntsman.drp.refcat import create_refcat_service


def run_nameserver():
    ns = NameServer(connect=False)
    ns.serve()


if __name__ == "__main__":

    # Start the pyro nameserver
    nameserver_proc = Process(target=run_nameserver)
    nameserver_proc.start()
    wait_for_nameserver()

    # Clear existing objects from nameserver
    ns = NameServer(connect=True)
    ns.clear()

    # Start the other pyro services
    refcat_service = create_refcat_service()
    refcat_service.start()

    # Run until nameserver process joins
    nameserver_proc.join()
    refcat_service.stop()
