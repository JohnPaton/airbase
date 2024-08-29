#! /usr/bin/env python3

"""See check_broken_links.py --help  for usage"""

import argparse
import os
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime

import requests
from tqdm import tqdm

import airbase as ab

REQUESTS_SESSION_CONNECTION_POOL_SIZE = 10


def main(output_file, retries, ignore_errors=False):
    """Check the entire AirBase database for broken links"""

    print(f"Will output bad links to {output_file}")

    client = ab.AirbaseClient()
    req = client.request(preload_urls=True)  # get links to all files
    session = requests.Session()  # reuse HTTP connections

    # Define inside main to re-use Session
    def is_404(url, r=retries):
        try:
            response = session.head(url, timeout=1)
            return response.status_code == 404
        except Exception:
            if r == 0 and not ignore_errors:
                raise
            elif r == 0 and ignore_errors:
                return None
            else:
                return is_404(url, r - 1)

    # clear output file
    try:
        os.remove(output_file)
    except FileNotFoundError:
        pass

    with ThreadPoolExecutor(REQUESTS_SESSION_CONNECTION_POOL_SIZE) as executor:
        promises = executor.map(
            is_404, tqdm(req._csv_links, desc="Creating queue")
        )

        total_bad = 0
        pbar = tqdm(total=len(req._csv_links), desc="Checking links")

        for i, not_found in enumerate(promises):
            pbar.update()

            if not_found:
                total_bad += 1
                with open(output_file, "a") as h:
                    h.write(req._csv_links[i] + "\n")
                pbar.set_description(f"{total_bad:,} bad links")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        usage="Test all the csv links from the airbase database to check for 404s",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    now = datetime.now().isoformat("T", "seconds")
    parser.add_argument(
        "-o",
        "--output",
        default=f"bad-links-{now}.txt",
        help="File to record the broken links in",
        type=argparse.FileType(),
    )
    parser.add_argument(
        "-r",
        "--retries",
        default=10,
        help="Number of times to retry a link in case of connection issues",
    )
    parser.add_argument(
        "-i",
        "--ignore-errors",
        action="store_true",
        default=False,
        help="Ignore connection errors (may result in an incomplete check)",
    )
    args = parser.parse_args()

    main(args.output, args.retries, args.ignore_errors)
