"""
Code to identify vignetting, particularly in flat fields.
"""


def calculate_asymmetry_statistics(data):
    """
    Calculate the asymmetry statistics by flipping data in x and y directions.
    """
    # Horizontal flip
    data_flip = data[:, ::-1]
    std_horizontal = (data-data_flip).std()

    # Vertical flip
    data_flip = data[::-1, :]
    std_vertical = (data-data_flip).std()

    return std_horizontal, std_vertical
