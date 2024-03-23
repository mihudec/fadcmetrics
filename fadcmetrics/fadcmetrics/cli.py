import asyncio
import sys
import pathlib
import argparse
import time

from fadcmetrics.utils.logging import get_logger
from fadcmetrics.config import FadcMetricsConfig, get_config
from fadcmetrics.metrics import FadcMetricsScraper

CWD = pathlib.Path.cwd()

def to_path(path_str: str):
    path = None
    path_candidate = pathlib.Path(path_str).resolve()
    if path_candidate.exists():
        path = path_candidate
    else:
        path_candidate = CWD.joinpath(path_str)
        if path_candidate.exists():
            path = path_candidate
        else:
            raise argparse.ArgumentTypeError("Path does not exist")
    return path

class FadcMetricsCli(object):

    def __init__(self) -> None:
        self.logger = get_logger(name=self.__class__.__name__)
        self.CONFIG: FadcMetricsConfig = None
        parser = argparse.ArgumentParser(
            description="",
        )
        parser.add_argument(
            '--config',
            help='Path to config file',
            type=to_path
        )
        args = parser.parse_args()
        try:
            self.CONFIG = get_config(args=args)
        except Exception as e:
            self.logger.error(msg=f"Failed to validate config. {repr(e)}")
            sys.exit(1)
        self.run_scrapers()

    def run_scrapers(self):
        scraper = FadcMetricsScraper(config=self.CONFIG)
        print(self.CONFIG.yaml())
        try:
            asyncio.run(scraper.run())
        except KeyboardInterrupt as e:
            print("Keyboard Interrupt - Exiting")
            time.sleep(5)
            

        
def main():
    FadcMetricsCli()

