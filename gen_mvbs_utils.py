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
    return (
        file_datetime.date() >= start.date()
        and file_datetime.date() <= end.date()
    )


def get_raw_file_url(
    file_url: str, start: dt.datetime, end: dt.datetime
) -> List[str]:
    """
    Obtain a list of raw files to be processed.

    Parameters
    ----------
    file_url : str
        url of the site http folder
    start : dt.datetime
        start date to access
    end : dt.datetime
        end date to access

    Returns
    -------
    A list of urls of the desired raw files

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


def get_daily_raw_file_url(
    file_url: str,
    d: dt.datetime,
    utc_offset: float
) -> List[str]:
    """
    Obtain a list of raw files for a specific date in local time.

    OOI records all files in UTC time, so we need to grab more than just
    files from a single UTC day.

    Parameters
    ----------
    d_wanted : dt.datetime
        Date wanted
    utc_offset : Union[int, float]
        Number of hours to offset from UTC
    """
    # Convert to UTC time
    start_utc = (
        d
        - dt.timedelta(hours=utc_offset)  # UTC offset
    )
    end_utc = (
        d + dt.timedelta(hours=23, minutes=59, seconds=59, milliseconds=999)
        - dt.timedelta(hours=utc_offset)  # UTC offset
        + dt.timedelta(days=1)  # padding to get range right
    )

    # Assemble day URLs
    all_dates = pd.date_range(start=start_utc, end=end_utc, freq="D")
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
        lambda raw_file: in_range(raw_file, start_utc, end_utc),
        all_raw_file_urls
    ))
