import pathlib
from setuptools import setup, find_packages

ROOT_DIR = pathlib.Path(__file__).parent.resolve()

VERSION = ROOT_DIR.joinpath("VERSION").read_text().strip()

def load_requirements():
    requirements = []
    REQS_PATH = ROOT_DIR.joinpath('requirements.txt')
    if REQS_PATH.exists() and REQS_PATH.is_file():
        requirements = [x for x in REQS_PATH.read_text().splitlines() if (len(x) and not x.startswith("#"))]
    return requirements

setup(
    name="fadcmetrics",
    packages=find_packages(),
    version=VERSION,
    author="Miroslav Hudec <http://github.com/mihudec>",
    description="FortiADC Metrics Scraper",
    install_requires=load_requirements(),
    include_package_data=True,
    entry_points = {
        'console_scripts': [
            'fadcmetrics = fadcmetrics.cli:main'
        ]
    }
)