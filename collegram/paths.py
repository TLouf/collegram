import re
from dataclasses import dataclass
from pathlib import Path
from string import Formatter

import collegram


def yield_kw_from_fmt_str(fmt_str):
    '''
    Yield all keywords from a format string.
    '''
    for _, fn, _, _ in Formatter().parse(fmt_str):
        if fn is not None:
            yield fn


def yield_paramed_matches(file_path_format, params_dict):
    '''
    Generator of named matches of the parameterised format `file_path_format`
    for every parameter not fixed in `params_dict`.
    '''
    fname = file_path_format.name
    # Create a dictionary with keys corresponding to each parameter of the file
    # format, and the values being either the one found in `params_dict`, or a
    # regex named capture group of the parameter.
    pformat = {fn: params_dict.get(fn, f'(?P<{fn}>.+)')
               for fn in yield_kw_from_fmt_str(fname)}
    file_pattern = re.compile(fname.replace('.', r'\.').format(**pformat))
    for f in file_path_format.parent.iterdir():
        match = re.search(file_pattern, f.name)
        if match is not None:
            yield match


def partial_format(fmt_str, *args, **kwargs):
    all_kw = list(yield_kw_from_fmt_str(fmt_str))
    fmt_dict = {**{kw: f'{{{kw}}}' for kw in all_kw}, **kwargs}
    return fmt_str.format(*args, **fmt_dict)


def partial_path_format(path_fmt, *args, **kwargs):
    return Path(partial_format(str(path_fmt), *args, **kwargs))


def format_path(path_fmt, *args, **kwargs):
    '''
    Utility to apply string formatting to a Path.
    '''
    return Path(str(path_fmt).format(*args, **kwargs))


def get_params_fmt_str(*param_names):
    return '_'.join("{0}={{{0}}}".format(p) for p in param_names)


def get_params_str(**kwargs):
    return '_'.join(f"{p}={v}" for p, v in kwargs.items())


@dataclass
class ProjectPaths:
    '''
    Dataclass containing all the paths used throughout the project. Defining
    this class allows us to define these only once, ensuring consistency.
    '''
    proj: Path = Path(collegram.__file__).parent.parent

    def __post_init__(
        self,
    ):
        self.proj_data = self.proj / 'data'
        self.ext_data = self.proj_data / 'external'
        self.raw_data = self.proj_data / 'raw'
        self.interim_data = self.proj_data / 'interim'
        self.processed_data = self.proj_data / 'processed'
        self.channel_seed = self.ext_data / 'channels.txt'
        self.figs = self.proj / 'reports' / 'figures'
