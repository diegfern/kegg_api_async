#!/usr/bin/env python3
import argparse
import asyncio
import csv
import functools
import itertools
import re
from pathlib import Path

import aiometer
import httpx

from helpers.utils import fetch_with_retry

DEBUG = False


async def main():
    args = parse_arguments()

    if Path(args.output).exists() and not args.force:
        print(f"{args.output} exists, not downloading")
        return

    if args.debug:
        global DEBUG
        DEBUG = True

    with open(args.input) as f:
        reader = csv.DictReader(f)
        organisms = [row["code"] for row in reader]

    client = httpx.AsyncClient(
        transport=httpx.AsyncHTTPTransport(retries=3),
        timeout=60,
        base_url="https://rest.kegg.jp",
    )
    async with aiometer.amap(
        functools.partial(fetch_pathways, client),
        organisms,
        max_per_second=8,
        max_at_once=9,
    ) as tasks:
        pathways = [x async for x in tasks]

    pathways_plain = sorted(
        list(itertools.chain.from_iterable(pathways)), key=lambda x: (x[0], x[1])
    )

    Path(args.output).parent.mkdir(parents=True, exist_ok=True)
    with open(args.output, "w") as f:
        writer = csv.writer(f)
        writer.writerow(["organism", "path_id", "description_path"])
        writer.writerows(pathways_plain)


async def fetch_pathways(client: httpx.AsyncClient, organism_code: str):
    api_path = f"/list/pathway/{organism_code}"
    response = await fetch_with_retry(client, api_path)

    pathways = [
        [organism_code, *re.split(r"\s*\t\s*", line.replace("path:", ""))]
        for line in response.text.splitlines()
    ]

    if DEBUG:
        print(f"{organism_code}: {len(pathways)} pathways")

    return pathways


def parse_arguments():
    parser = argparse.ArgumentParser(description="Get organisms from KEGG")
    parser.add_argument("-i", "--input", required=True, help="Organisms file")
    parser.add_argument("-o", "--output", required=True, help="Output file")
    parser.add_argument(
        "-d", "--debug", action="store_true", help="Show debug messages"
    )
    parser.add_argument(
        "-f",
        "--force",
        action="store_true",
        help="Force download even if output file exists",
    )

    return parser.parse_args()


if __name__ == "__main__":
    asyncio.run(main())
