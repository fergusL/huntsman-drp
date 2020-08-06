from setuptools import setup, find_packages

packages = find_packages()

setup(name='huntsman-drp',
      packages=packages,
      package_dir={'': 'src'},
      zip_safe=False,
      )
