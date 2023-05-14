#!/usr/bin/env bash

export DEBIAN_FRONTEND=noninteractive
sudo apt update && sudo apt upgrade -yq
sudo apt install -yq python3-dev python3-pip virtualenv

# copy everything to vps with
# scp -r . ubuntu@${VPS_IP}:kegg_api

virtualenv ./venv
source ./venv/bin/activate
pip install -r ./requirements.txt