
from random import randint
import os
from astropy.io import fits

from collections import OrderedDict
from photutils.datasets import (make_gaussian_sources_image, make_noise_image,
                                make_random_gaussians_table)


def make_fake_image(temp_directory,
                    file_name,
                    num_images=1,
                    n_sources=30,
                    flux_range=[500, 1000],
                    shape=(300, 500),
                    background=0,
                    stddev=1,
                    xstddev_range=[1, 5],
                    ystddev_range=[1, 5]):
    """
    Identical to photutils.datasets.make_100gaussians_image, except
    makes an example image containing 30 2D Gaussians with the
    positions determined by a input seed. The photutils version has a
    hard-coded seed.

    The background has a mean of 5 and a standard deviation of 2.

    Parameters
    ----------
    seed : int
        Integer to use as seed. If None is given, a random int will be
        used instead.

    Returns
    -------
    image : `~numpy.ndarray`
        Image containing Gaussian sources.

    See Also
    --------
    make_4gaussians_image

    Examples
    --------
    .. plot::
        :include-source:

        from photutils import datasets
        image = datasets.make_30gaussians_image()
        plt.imshow(image, origin='lower', cmap='gray')
    """
    xmean_range = [0, shape[1]]
    ymean_range = [0, shape[0]]

    param_ranges = OrderedDict([('amplitude', flux_range),
                                ('x_mean', xmean_range),
                                ('y_mean', ymean_range),
                                ('x_stddev', xstddev_range),
                                ('y_stddev', ystddev_range),
                                ('theta', [0, 2 * 3.14])])

    try:
        os.makedirs(temp_directory)
    except FileExistsError:
        pass

    images = []
    filenames = []
    for num in range(0, num_images):
        table = make_random_gaussians_table(
            n_sources, param_ranges, random_state=randint(flux_range[0], flux_range[1]))
        image1 = make_gaussian_sources_image(shape, table)
        image2 = image1 + make_noise_image(shape, distribution='gaussian',
                                           mean=background, stddev=stddev,
                                           random_state=randint(flux_range[0], flux_range[1]))
        hdu = fits.PrimaryHDU(image2)
        hdu.header['EGAIN'] = 1.
        hdu.header['EXPTIME'] = 1.
        hdu.header['FILTER'] = 'g'
        hdu.header['FIELD'] = 'test_target'
        hdu.header['SBIG-ID'] = 'camera'
        hdu.header['SEQID'] = 'sequence'
        hdu.header['IMAGEID'] = str(num)
        hdu.header['DATE-OBS'] = '2017-04-14T09:29:48.033(UTC)'
        output_filename = os.path.join(str(temp_directory), '{}_{}.fits'.format(file_name, num))
        hdu.writeto(output_filename, overwrite=True)
        images.append(hdu)
        filenames.append(output_filename)
    return images, filenames
