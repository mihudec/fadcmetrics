import sys
import pathlib
import json
import yaml

from argparse import Namespace
from pydantic import BaseSettings, Field, validator, root_validator
from pydantic.typing import Any, Dict, List, Literal, Optional, Union
from fadcmetrics.utils.logging import get_logger

LOGGER = get_logger(name="FadcMetricsConfig")


DEFAULT_CONFIG_PATH = pathlib.Path.home().joinpath('.fadcmetrics.yml')


class ConfigBase(BaseSettings):

    class Config:
        validate_assignment = True


    @classmethod
    def from_yaml(cls, text: str):
        data = yaml.safe_load(text)
        config = None
        try:
            config = data['config']
        except KeyError as e:
            LOGGER.critical(msg="YAML Config does not contain required key 'config'")
        except AttributeError as e:
            LOGGER.critical(msg="Could not load YAML Config. Please check the config file.")
        except Exception as e:
            LOGGER.critical(msg=f"Unhandled Exception occurred while building TIL Config. {repr(e)}")
        if config is not None:
            config = cls.parse_obj(config)
        else:
            LOGGER.critical(msg="Got Empty Config...")
        return config

    def sdict(self):
        # Return serialized dict
        sdict = json.loads(self.json(exclude_none=True))
        return sdict

    def yaml(self):
        return yaml.safe_dump(data=self.sdict())

    @classmethod
    def from_file(cls, path: pathlib.Path):
        if not isinstance(path, pathlib.Path):
            path = pathlib.Path(path).resolve()
        text = None
        config = None
        if not path.exists():
            LOGGER.error(f"Given path for config does not exist: {path}")
        else:
            text = path.read_text()
        if path.suffix in ['.yml', '.yaml']:
            config = cls.from_yaml(text=text)
        else:
            LOGGER.error(msg=f"Cannot determine config file format based on suffix, got {path.suffix=}")
        return config

class ScrapeConfig(ConfigBase):
    topic: Literal['vs_status', 'vs_http_stats']
    tags: Optional[Dict[str, str]]

class TargetConfig(ConfigBase):

    hostname: str
    base_url: str
    username: str
    password: str
    verify_ssl: bool = True
    scrape_interval: int
    scrape_configs: List[ScrapeConfig]
    tags: Optional[Dict[str, str]]

    @root_validator(pre=False)
    def use_hostname_as_tag(cls, values):
        tags = values.get('tags')
        if tags is None:
            tags = dict()
        if tags.get('hostname') is None:
            tags['hostname'] = values.get('hostname')
        values['tags'] = tags
        return values

class FadcMetricsConfig(ConfigBase):

    targets: List[TargetConfig]

def get_config(args: Union[Dict, Namespace] = Namespace()):
    global LOGGER
    if isinstance(args, dict):
        args = Namespace(args)
    config_file_path = getattr(args, 'config', None)
    if config_file_path is None:
        config_file_path = DEFAULT_CONFIG_PATH

    config = None

    if config_file_path.exists():
        LOGGER.debug(msg=f"Settings file {config_file_path} exists, loading_settings.")
        config = FadcMetricsConfig.from_file(path=config_file_path)
        if config is None:
            LOGGER.critical("Failed to load settings, exiting.")
            sys.exit(1)
    else:
        LOGGER.debug(msg=f"Settings file {config_file_path} does not exists, using defaults.")
        config = FadcMetricsConfig()

    for field_name in FadcMetricsConfig.__fields__.keys():
        value = getattr(args, field_name, None)
        if value is not None:
            setattr(config, field_name, value)
    

    return config
