from __future__ import annotations

import json
from pathlib import Path

import pytest

from airbase.download_api import (
    Dataset,
    DownloadInfo,
    DownloadSession,
)


@pytest.fixture(scope="module")
def session():
    return DownloadSession()


def test_Dataset():
    assert Dataset.Historical == Dataset.Airbase == 3
    assert Dataset.Verified == Dataset.E1a == 2
    assert Dataset.Unverified == Dataset.UDT == Dataset.E2a == 1
    assert (
        json.dumps(list(Dataset)) == json.dumps(tuple(Dataset)) == "[3, 2, 1]"
    )


@pytest.mark.parametrize(
    "pollutant,country,city,historical,verified,unverified",
    (
        pytest.param(
            "PM10",
            "NO",
            None,
            '{"countries": ["NO"], "cities": [], "properties": ["PM10"], "datasets": [3], "source": "API"}',
            '{"countries": ["NO"], "cities": [], "properties": ["PM10"], "datasets": [2], "source": "API"}',
            '{"countries": ["NO"], "cities": [], "properties": ["PM10"], "datasets": [1], "source": "API"}',
            id="PM10-NO",
        ),
        pytest.param(
            "O3",
            "IS",
            "Reykjavik",
            '{"countries": ["IS"], "cities": ["Reykjavik"], "properties": ["O3"], "datasets": [3], "source": "API"}',
            '{"countries": ["IS"], "cities": ["Reykjavik"], "properties": ["O3"], "datasets": [2], "source": "API"}',
            '{"countries": ["IS"], "cities": ["Reykjavik"], "properties": ["O3"], "datasets": [1], "source": "API"}',
            id="O3-IS",
        ),
    ),
)
def test_DownloadInfo(
    pollutant: str,
    country: str,
    city: str | None,
    historical: str,
    verified: str,
    unverified: str,
):
    assert (
        json.dumps(
            DownloadInfo(
                pollutant, country, Dataset.Historical, city
            ).request_info()
        )
        == historical
    ), "unexpected historical info"
    assert (
        json.dumps(
            DownloadInfo(
                pollutant, country, Dataset.Verified, city
            ).request_info()
        )
        == verified
    ), "unexpected verified info"
    assert (
        json.dumps(
            DownloadInfo(
                pollutant, country, Dataset.Unverified, city
            ).request_info()
        )
        == unverified
    ), "unexpected unverified info"


@pytest.mark.asyncio
async def test_url_to_files(session: DownloadSession):
    async with session:
        urls = await session.url_to_files(
            DownloadInfo("O3", "NO", Dataset.Historical, "Oslo")
        )
    for url in urls:
        assert url.startswith("https://"), f"wrong {url=}"
    assert len(urls) == 56


@pytest.mark.asyncio
async def test_download_to_directory(session: DownloadSession, tmp_path: Path):
    assert not tuple(tmp_path.glob("NO/*.parquet"))
    urls = (
        "https://eeadmz1batchservice02.blob.core.windows.net/airquality-p-airbase/NO/SPO-NO0010A_00005_501.parquet",
        "https://eeadmz1batchservice02.blob.core.windows.net/airquality-p-airbase/NO/SPO-NO0010A_00005_503.parquet",
        "https://eeadmz1batchservice02.blob.core.windows.net/airquality-p-airbase/NO/SPO-NO0010A_00005_504.parquet",
        "https://eeadmz1batchservice02.blob.core.windows.net/airquality-p-airbase/NO/SPO-NO0010A_00008_500.parquet",
        "https://eeadmz1batchservice02.blob.core.windows.net/airquality-p-airbase/NO/SPO-NO0010A_00009_500.parquet",
        "https://eeadmz1batchservice02.blob.core.windows.net/airquality-p-airbase/NO/SPO-NO0011A_00020_100.parquet",
    )
    async with session:
        await session.download_to_directory(
            tmp_path, *urls, raise_for_status=True
        )
    assert len(tuple(tmp_path.glob("NO/*.parquet"))) == len(urls)
