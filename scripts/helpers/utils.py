import asyncio
import os
import re
from random import random

import httpx

REGEX_FASTA = re.compile(r">(.+)\n([A-Z\n]+)")
REGEX_EC_NUMBERS = re.compile(r"(?<=\[EC:)([\d .-]+)(?=])")


async def fetch_with_retry(client: httpx.AsyncClient, path: str) -> httpx.Response:
    """
    Fetch a resource, retrying with waiting if the server returns a 403 error
    """
    response = await client.get(path)

    # Handle permission denied on the API
    sleep_seconds = 4 + random() * 2
    while response.status_code == 403:
        if "DEBUG" in os.environ and os.environ["DEBUG"] == "1":
            print(f"Waiting for {sleep_seconds} seconds")
        await asyncio.sleep(sleep_seconds)
        response = await client.get(path)
        sleep_seconds *= 2

    return response


def get_kegg_section_regex(section: str) -> re.Pattern:
    """
    Get a regex for a KEGG section
    """
    return re.compile(r"(?<=" + section + r")(.|\n )+(?=\n[A-Z]+)")
