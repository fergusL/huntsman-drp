from huntsman.drp.utils import load_yaml, load_module

DEFAULT_REDUCTION_TYPE = "huntsman.drp.reduction.lsst.LsstDataReduction"


def create_from_file(filename, **kwargs):
    """ Create a reduction instance from a config file. """
    cfg = load_yaml(filename)
    cfg.update(kwargs)  # Override config with keyword args

    class_type = cfg.pop("type", DEFAULT_REDUCTION_TYPE)
    ReductionClass = load_module(class_type)

    return ReductionClass(**cfg)
