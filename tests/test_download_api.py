from __future__ import annotations

import json
from pathlib import Path
from textwrap import dedent

import httpx
import pytest
from pytest_httpx import HTTPXMock

from airbase.download_api import (
    Dataset,
    DownloadAPIClient,
    DownloadInfo,
    get_client,
)


def test_Dataset():
    assert Dataset.Historical == Dataset.Airbase == 3
    assert Dataset.Verified == Dataset.E1a == 2
    assert Dataset.Unverified == Dataset.UDT == Dataset.E2a == 1
    assert (
        json.dumps(list(Dataset)) == json.dumps(tuple(Dataset)) == "[3, 2, 1]"
    )


@pytest.mark.parametrize(
    "pollutant,country,cities,historical,verified,unverified",
    (
        pytest.param(
            "PM10",
            "NO",
            tuple(),
            '{"countries": ["NO"], "cities": [], "properties": ["PM10"], "datasets": [3], "source": "API"}',
            '{"countries": ["NO"], "cities": [], "properties": ["PM10"], "datasets": [2], "source": "API"}',
            '{"countries": ["NO"], "cities": [], "properties": ["PM10"], "datasets": [1], "source": "API"}',
            id="PM10-NO",
        ),
        pytest.param(
            "O3",
            "IS",
            ("Reykjavik",),
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
    cities: tuple[str, ...],
    historical: str,
    verified: str,
    unverified: str,
):
    assert (
        json.dumps(
            DownloadInfo.historical(pollutant, country, *cities).request_info()
        )
        == historical
    ), "unexpected historical info"
    assert (
        json.dumps(
            DownloadInfo.verified(pollutant, country, *cities).request_info()
        )
        == verified
    ), "unexpected verified info"
    assert (
        json.dumps(
            DownloadInfo.unverified(pollutant, country, *cities).request_info()
        )
        == unverified
    ), "unexpected unverified info"


@pytest.mark.asyncio
class TestClient:
    async def test_num_files_processing(self, httpx_mock: HTTPXMock):
        num_files = 10
        num_urls = 3

        httpx_mock.add_response(json={"numberFiles": num_files, "size": 123456})

        client = get_client()
        result = await client.total_num_parquet_files(
            *[DownloadInfo.historical("O3", "NL") for _ in range(num_urls)]
        )
        assert result == num_urls * num_files

    async def test_parquet_file_url_processing(self, httpx_mock: HTTPXMock):
        num_urls = 3

        httpx_mock.add_response(
            text=dedent("""\
                ParquetFileUrl
                https://example.com/1.parquet\r
                https://example.com/2.parquet\r
                https://example.com/3.parquet\r
            """)
        )

        client = get_client()
        result_urls = []
        async for result in client.parquet_file_urls(
            *[DownloadInfo.historical("O3", "NL") for _ in range(num_urls)]
        ):
            result_urls.append(result)

        assert len(result_urls) == num_urls * 3

    async def test_pollutants(self, httpx_mock: HTTPXMock):
        httpx_mock.add_response(
            json=[
                dict(
                    notation="O3",
                    id="http://dd.eionet.europa.eu/vocabulary/aq/pollutant/1",
                ),
                dict(
                    notation="O3",
                    id="http://dd.eionet.europa.eu/vocabulary/aq/pollutant/2",
                ),
                dict(
                    notation="NO2",
                    id="http://dd.eionet.europa.eu/vocabularyconcept/aq/pollutant/44/view",
                ),
            ]
        )

        client = get_client()
        result = await client.pollutants()
        assert result == {"O3": {1, 2}, "NO2": {44}}

    async def test_country(self, httpx_mock: HTTPXMock):
        countries = ["A", "B", "C"]
        httpx_mock.add_response(
            json=[{"countryCode": country} for country in countries]
        )

        client = get_client()
        result = await client.countries()
        assert result == countries

    async def test_cities(self, httpx_mock: HTTPXMock):
        httpx_mock.add_response(
            json=[
                {"countryCode": "NL", "cityName": "Amsterdam"},
                {"countryCode": "NL", "cityName": "Rotterdam"},
                {"countryCode": "DE", "cityName": "Berlin"},
            ],
        )

        client = get_client()
        result = await client.cities("NL", "DE")
        assert result == {
            "NL": {"Amsterdam", "Rotterdam"},
            "DE": {"Berlin"},
        }

    async def test_download_url_to_directory(
        self, httpx_mock: HTTPXMock, tmp_path
    ):
        destination = Path(tmp_path)
        content = b"testing"
        filename = "testing.txt"
        output_file = destination / filename

        httpx_mock.add_response(content=content)

        client = get_client()
        await client._download_url_to_directory(
            f"https://example.com/{filename}", destination=destination
        )
        assert output_file.exists()
        assert output_file.read_bytes() == content

    async def test_download(self, httpx_mock: HTTPXMock, tmp_path):
        destination = Path(tmp_path)
        content = b"testing"
        num_files = 3

        client = get_client()

        url_list = "\r\n".join(
            ["ParquetFileUrl"]
            + [f"https://example.com/{i}.parquet" for i in range(num_files)]
        )

        httpx_mock.add_response(
            url=f"{client.base_url}/ParquetFile/urls",
            text=url_list,
        )

        for i in range(num_files):
            httpx_mock.add_response(
                url=f"https://example.com/{i}.parquet", content=content
            )

        await client.download(
            DownloadInfo.historical("O3", "NL"), destination=destination
        )

        for i in range(num_files):
            file = destination / f"{i}.parquet"
            assert file.exists()
            assert file.read_bytes() == content

    @pytest.mark.parametrize("progress", [True, False])
    async def test_download_progress(
        self, httpx_mock: HTTPXMock, tmp_path, progress, capsys
    ):
        destination = Path(tmp_path)
        content = b"testing"
        num_files = 3

        client = get_client()
        client.progress = progress

        url_list = "\r\n".join(
            ["ParquetFileUrl"]
            + [f"https://example.com/{i}.parquet" for i in range(num_files)]
        )

        httpx_mock.add_response(
            url=f"{client.base_url}/ParquetFile/urls",
            text=url_list,
        )

        if progress:
            httpx_mock.add_response(
                url=f"{client.base_url}/DownloadSummary",
                json={"numberFiles": num_files, "size": 123456},
            )

        for i in range(num_files):
            httpx_mock.add_response(
                url=f"https://example.com/{i}.parquet", content=content
            )

        await client.download(
            DownloadInfo.historical("O3", "NL"), destination=destination
        )

        captured = capsys.readouterr()
        if progress:
            assert "Downloading parquets" in captured.err
        else:
            assert captured.out == ""
            assert captured.err == ""

    async def test_raise_for_status(self, httpx_mock: HTTPXMock):
        httpx_mock.add_response(status_code=429)
        client = DownloadAPIClient(raise_for_status=True)

        with pytest.raises(httpx.HTTPStatusError):
            await client.get("https://example.com")

    async def test_warn_for_status(self, httpx_mock: HTTPXMock):
        httpx_mock.add_response(status_code=429)
        client = DownloadAPIClient(raise_for_status=False)

        with pytest.warns(RuntimeWarning):
            await client.get("https://example.com")
