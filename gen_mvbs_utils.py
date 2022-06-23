from pathlib import Path
import itertools as it
import datetime as dt
from typing import List
from dateutil import parser as dtparser

import pandas as pd
import fsspec


def in_range(raw_file: str, start: dt.datetime, end: dt.datetime) -> bool:
    """
    Check if a file url is within the specified datetime range.
    """
    file_name = Path(raw_file).name
    file_datetime = dtparser.parse(file_name, fuzzy=True)
    return file_datetime >= start and file_datetime <= end


def get_raw_file_url(file_url: str, start: dt.datetime, end: dt.datetime) -> List[str]:
    """
    Obtain a list of raw files to be processed.

    Notes
    -----
    Since time is in UTC, often it is useful to specify the start and end dates
    to be a day earlier and later than the desired date range to ensure the
    entire desired data section is included.
    """
    # Assemble day URLs
    all_dates = pd.date_range(start=start, end=end, freq="D")
    desired_day_urls = [
        f"{file_url}/{d.year}/{d.month:02d}/{d.day:02d}"
        for d in all_dates
    ]

    # Get complete path to raw file
    fs = fsspec.filesystem('https')
    all_raw_file_urls = it.chain.from_iterable(
        [fs.glob(f"{day_url}/*.raw") for day_url in desired_day_urls]
    )
    return list(filter(
        lambda raw_file: in_range(raw_file, start, end), 
        all_raw_file_urls
    ))
