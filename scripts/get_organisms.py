#!/usr/bin/env python3
import argparse
import asyncio
import csv
import re
from pathlib import Path

import httpx


async def main():
    args = parse_arguments()

    if Path(args.output).exists() and not args.force:
        print(f"{args.output} exists, not downloading")
        return

    async with httpx.AsyncClient() as client:
        response = await client.get("https://rest.kegg.jp/list/organism")

    organisms = [re.split(r"\s*\t\s*", line) for line in response.text.splitlines()]
    organisms = sorted(organisms, key=lambda x: x[0])

    Path(args.output).parent.mkdir(parents=True, exist_ok=True)
    with open(args.output, "w") as f:
        writer = csv.writer(f, quoting=csv.QUOTE_MINIMAL)
        writer.writerow(["id_code", "code", "name_organism", "description"])
        writer.writerows(organisms)


def parse_arguments():
    parser = argparse.ArgumentParser(description="Get organisms from KEGG")
    parser.add_argument("-o", "--output", required=True, help="Output file")
    parser.add_argument(
        "-f",
        "--force",
        action="store_true",
        help="Force download even if output file exists",
    )

    return parser.parse_args()


if __name__ == "__main__":
    asyncio.run(main())
