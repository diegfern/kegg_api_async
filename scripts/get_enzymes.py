#!/usr/bin/env python3
import argparse
import asyncio
import csv
import functools
import itertools
from dataclasses import dataclass
from pathlib import Path
from typing import List, Optional, Tuple

import aiofiles
import aiometer
import httpx

from helpers.utils import REGEX_EC_NUMBERS, fetch_with_retry, get_kegg_section_regex

DEBUG = False
REGEX_GENE_SECTION = get_kegg_section_regex("GENE")


@dataclass
class Pathway:
    id: str
    organism: str


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
        pathways = [
            Pathway(id=row["path_id"], organism=row["organism"]) for row in reader
        ]

    cache_path = Path(args.cache_dir) / "pathway" if args.cache_dir else None

    # search for files in the cache first
    cached_enzymes = []
    if cache_path is not None:
        pathways, cached_enzymes = await fetch_cache(pathways, cache_path)

    client = httpx.AsyncClient(
        transport=httpx.AsyncHTTPTransport(retries=3),
        timeout=60,
        base_url="https://rest.kegg.jp",
    )
    async with aiometer.amap(
        functools.partial(fetch_pathway, client, cache_path),
        pathways,
        max_per_second=8,
        max_at_once=9,
    ) as tasks:
        enzymes = [x async for x in tasks]

    # convert enzymes to a plain list and sort them
    enzymes_plain = sorted(
        list(itertools.chain.from_iterable(cached_enzymes + enzymes))
    )

    Path(args.output).parent.mkdir(parents=True, exist_ok=True)
    with open(args.output, "w") as f:
        writer = csv.writer(f)
        writer.writerow(["organism", "pathway", "code_enzyme"])
        writer.writerows(enzymes_plain)


async def fetch_pathway(
    client: httpx.AsyncClient, cache_path: Optional[Path], pathway: Pathway
):
    """
    Fetch pathways from KEGG API and extract the enzymes
    """
    response = await fetch_with_retry(client, f"/get/{pathway.id}")

    # save the raw response in a text file
    if cache_path is not None:
        save_fullpath = cache_path / pathway.organism
        save_fullpath.mkdir(parents=True, exist_ok=True)

        async with aiofiles.open(save_fullpath / f"{pathway.id}.txt", "w") as f:
            await f.write(response.text)

    # parse EC numbers
    ec_numbers = sorted(list(set(REGEX_EC_NUMBERS.findall(response.text))))

    if DEBUG:
        print(f"{pathway}: {len(ec_numbers)} EC numbers")

    return [[pathway.organism, pathway.id, n] for n in ec_numbers]


async def fetch_cache(
    pathways: List[Pathway], cache_path: Path
) -> Tuple[List[Pathway], list]:
    """
    Search for enzymes in the cache, and retrieve the ones found
    """
    cache_queries = [
        functools.partial(_get_pathway_from_cache, cache_path, p) for p in pathways
    ]
    cache_results = await aiometer.run_all(cache_queries, max_at_once=100)

    # get cached enzymes from results
    cached_enzymes = [x for x in cache_results if x is not None]
    # keep the pathways that were not found in the cache
    filtered_pathways = [p for p, r in zip(pathways, cache_results) if r is None]

    if DEBUG:
        print(f"{len(cached_enzymes)} / {len(pathways)} pathways found in cache")

    return filtered_pathways, cached_enzymes


async def _get_pathway_from_cache(cache_path: Path, pathway: Pathway) -> Optional[list]:
    fullpath = cache_path / pathway.organism / f"{pathway.id}.txt"
    if not fullpath.is_file():
        return None

    async with aiofiles.open(fullpath, "r") as f:
        text = await f.read()
    ec_numbers = sorted(list(set(REGEX_EC_NUMBERS.findall(text))))

    return [[pathway.organism, pathway.id, n] for n in ec_numbers]


def parse_arguments():
    parser = argparse.ArgumentParser(description="Get enzymes from KEGG")
    parser.add_argument("-i", "--input", required=True, help="Pathways file")
    parser.add_argument("-o", "--output", required=True, help="Output file")
    parser.add_argument(
        "-c",
        "--cache-dir",
        required=False,
        help="Cache directory for KEGG API responses",
    )
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
