from huntsman.drp.core import get_config


def get_camera_configs(config=None):
    """ Load the camera configs from the config file.
    Args:
        config (abc.Mapping): The config dict.
    Returns:
        list of dict: List of camera configs.
    """
    if config is None:
        config = get_config()

    default_preset = config["cameras"]["default_preset"]

    camera_configs = []
    for device_info in config["cameras"]["devices"]:
        camera_config = {}
        preset = device_info.get("preset", default_preset)

        # Load the camera preset config
        camera_config.update(config["cameras"]["presets"][preset])

        # Apply any instance-specific overrides
        camera_config.update(device_info)

        camera_configs.append(camera_config)

    return camera_configs
