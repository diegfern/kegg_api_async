#!/usr/bin/env bash

# install dependencies
# pip install -r requirements.txt


# get organism list
./scripts/get_organisms.py -o ./data/organisms.csv

# get pathway list
./scripts/get_pathways.py -o ./data/pathways.csv -i ./data/organisms.csv -d

# get enzymes
# o pathways?
./scripts/get_enzymes.py -i ./data/pathways_split/pathways.csv -o ./data/enzymes.csv -s ./data/cache/ -d

# next, get the unique list of EC numbers from the csv

# get sequences
./scripts/get_sequences.py -i ./data/enzymes_unique.txt -o ./data/sequences.csv -c ./data/cache/ -d