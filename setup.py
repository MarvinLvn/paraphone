# Always prefer setuptools over distutils
from pathlib import Path

from setuptools import setup, find_packages

here = Path(__file__).parent.absolute()

# Get the long description from the README file
with open(here / Path('README.md')) as f:
    long_description = f.read()

with open(here / Path('requirements.txt')) as f:
    requirements = f.read().split("\n")

dependency_links = [req for req in requirements if req.startswith('git+ssh')]
requirements = [req for req in requirements if not req.startswith('git+ssh')]

setup(
    name='paraphone',
    version='0.1.0',
    description='',
    long_description=long_description,
    long_description_content_type="text/markdown",
    url='',
    author='Hadrien Titeux',
    author_email='hadrien.titeux@ens.fr',
    license="MIT",
    classifiers=[
        'Development Status :: 3 - Alpha',
        'License :: OSI Approved :: MIT License',
        'Intended Audience :: Developers',
        'Intended Audience :: Science/Research',
        'Programming Language :: Python :: 3.8'
        'Programming Language :: Python :: 3.9'
    ],
    keywords='',
    packages=find_packages(exclude=['docs', 'tests']),
    setup_requires=['setuptools>=38.6.0'],  # >38.6.0 needed for markdown README.md
    install_requires=requirements,
    dependency_links=dependency_links,
    extras_require={
        "habilis": [
            # needed for habilis
            "tensorflow==1.8.0",
            "tensorflow-gpu>=1.8.0"
        ],
    }
)
