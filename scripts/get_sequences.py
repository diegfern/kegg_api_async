#!/usr/bin/env python3
import argparse
import asyncio
import csv
import functools
import itertools
import os
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

import aiofiles
import aiometer
import httpx

from helpers.utils import REGEX_FASTA, fetch_with_retry, get_kegg_section_regex

DEBUG = False
REGEX_GENES_SECTION = get_kegg_section_regex("GENES")


@dataclass
class Sequence:
    organism: str
    id: str


async def main():
    args = parse_arguments()

    if Path(args.output).exists() and not args.force:
        print(f"{args.output} exists, not downloading")
        return

    if args.debug:
        global DEBUG
        DEBUG = True
        os.environ["DEBUG"] = "1"

    with open(args.input) as f:
        ec_numbers = f.read().splitlines()

    cache_path = Path(args.cache_dir) if args.cache_dir else None

    client = httpx.AsyncClient(
        transport=httpx.AsyncHTTPTransport(retries=3),
        timeout=60,
        base_url="https://rest.kegg.jp",
    )

    # fetch genes from cache
    cached_genes = []
    if cache_path is not None:
        ec_numbers, cached_genes = await fetch_cache(
            ec_numbers, cache_path / "enzyme", _get_enzyme_from_cache, "enzymes"
        )

    # fetch genes from API
    async with aiometer.amap(
        functools.partial(fetch_enzyme, client, cache_path / "enzyme"),
        ec_numbers,
        max_per_second=8,
        max_at_once=9,
    ) as tasks:
        genes = [x async for x in tasks if x is not None]

    # convert genes to a plain list
    genes_plain = list(itertools.chain.from_iterable(cached_genes + genes))

    # deduplicate and sort sequences
    unique_sequences = sorted(list(set((g[1], g[2]) for g in genes_plain)))
    sequences_to_fetch = [Sequence(*s) for s in unique_sequences]

    # fetch sequences from cache
    cached_sequences = []
    if cache_path is not None:
        sequences_to_fetch, cached_sequences = await fetch_cache(
            sequences_to_fetch,
            cache_path / "aaseq",
            _get_sequence_from_cache,
            "sequences",
        )

    # fetch sequences from API
    async with aiometer.amap(
        functools.partial(fetch_sequence, client, cache_path / "aaseq"),
        sequences_to_fetch,
        max_per_second=8,
        max_at_once=9,
    ) as tasks:
        sequences = [x async for x in tasks if x is not None]

    # merge sequences to the rest of the data
    sequence_dict = {
        (s[0].organism, s[0].id): s[1:] for s in sequences + cached_sequences
    }
    for gene in genes_plain:
        sequence = sequence_dict.get((gene[1], gene[2]))
        if sequence is not None:
            gene += sequence
        else:
            print(f"Could not find sequence for {gene[1]}:{gene[2]}")

    Path(args.output).parent.mkdir(parents=True, exist_ok=True)
    with open(args.output, "w") as f:
        writer = csv.writer(f)
        writer.writerow(
            [
                "enzyme_code",
                "organism",
                "sequence_id",
                "sequence_description",
                "sequence_aa",
            ]
        )
        writer.writerows(genes_plain)


async def fetch_enzyme(
    client: httpx.AsyncClient, cache_path: Optional[Path], ec_number: str
) -> Optional[list]:
    """
    Fetch enzymes from KEGG API and extract the enzymes
    """
    # Try up to 3 times to fetch the pathway
    for _ in range(3):
        response = await fetch_with_retry(client, f"/get/ec:{ec_number}")
        # check if the response is the expected one
        if (
            response.status_code == 200
            and response.text.startswith("ENTRY")
            and re.search(r"///\s+$", response.text)
            and ec_number in response.text
        ):
            break
    else:
        print(f"Error fetching {ec_number}")
        return None

    # save the raw response in a text file
    if cache_path is not None:
        ec_split = ec_number.split(".")
        save_fullpath = cache_path.joinpath(*ec_split[:-1])
        save_fullpath.mkdir(parents=True, exist_ok=True)

        async with aiofiles.open(save_fullpath / f"{ec_number}.txt", "w") as f:
            await f.write(response.text)

    # parse EC numbers
    genes = parse_genes(ec_number, response.text)

    if DEBUG:
        print(f"{ec_number}: {len(genes)} genes")

    return genes


async def fetch_sequence(
    client: httpx.AsyncClient, cache_path: Optional[Path], sequence: Sequence
) -> Optional[tuple]:
    """
    Fetch sequences from KEGG API and extract the sequences
    """
    api_path = f"/get/{sequence.organism}:{sequence.id}/aaseq"

    # try up to 3 times to get the sequence
    for _ in range(3):
        response = await fetch_with_retry(client, api_path)

        try:
            seq_id, seq = REGEX_FASTA.search(response.text).groups()

            if seq_id != "" and seq != "":
                break
        except AttributeError:
            continue
    else:
        print(f"Could not get sequence {sequence.organism}:{sequence.id}")
        return None

    # save the raw response in a text file
    if cache_path is not None:
        save_fullpath = cache_path / sequence.organism
        save_fullpath.mkdir(parents=True, exist_ok=True)

        async with aiofiles.open(save_fullpath / f"{sequence.id}.txt", "w") as f:
            await f.write(response.text)

    if DEBUG:
        print(f"Got sequence {sequence.organism}:{sequence.id}")

    seq = re.sub("[^A-Z]+", '', seq)

    return sequence, seq_id, seq


async def fetch_cache(
    elements: list,
    cache_path: Path,
    get_function: callable,
    title: Optional[str] = None,
) -> (list, list):
    """
    Fetch elements from the cache
    """
    cache_queries = [functools.partial(get_function, cache_path, e) for e in elements]
    cache_results = await aiometer.run_all(cache_queries, max_at_once=100)

    # get cache hits
    cached_elements = [x for x in cache_results if x is not None]
    # get cache misses
    uncached_elements = [e for e, r in zip(elements, cache_results) if r is None]

    title = title if title is not None else "elements"
    if DEBUG:
        print(f"{len(cached_elements)} / {len(elements)} {title} found in cache")

    return uncached_elements, cached_elements


async def _get_enzyme_from_cache(cache_path: Path, ec_number: str) -> Optional[list]:
    """
    Check if an enzyme is in the cache
    """
    ec_split = ec_number.split(".")
    fullpath = cache_path.joinpath(*ec_split[:-1]) / f"{ec_number}.txt"
    if not fullpath.is_file():
        return None

    async with aiofiles.open(fullpath, "r") as f:
        text = await f.read()

    return parse_genes(ec_number, text)


async def _get_sequence_from_cache(
    cache_path: Path, sequence: Sequence
) -> Optional[tuple]:
    """
    Check if a sequence is in the cache
    """
    fullpath = cache_path / sequence.organism / f"{sequence.id}.txt"
    if not fullpath.is_file():
        return None

    async with aiofiles.open(fullpath, "r") as f:
        text = await f.read()

    seq_id, seq = REGEX_FASTA.search(text).groups()

    return sequence, seq_id, seq


def parse_genes(ec_number: str, text: str) -> list:
    """
    Parse the genes from a KEGG API response
    """
    genes_section = REGEX_GENES_SECTION.search(text)
    if genes_section is None:
        return []

    genes = []
    regex_gene_row = re.compile(r"([A-Z]+): (.+)")
    regex_parentheses = re.compile(r"\(.+\)")
    for line in genes_section.group(0).split("\n"):
        organism, gene_list = regex_gene_row.search(line).groups()
        gene_list = regex_parentheses.sub("", gene_list)
        genes += [[ec_number, organism.lower(), g] for g in gene_list.split(" ")]

    return genes


def parse_arguments():
    parser = argparse.ArgumentParser(description="Get sequences from KEGG")
    parser.add_argument("-i", "--input", required=True, help="Enzyme list")
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
