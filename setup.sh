#!/bin/sh
virtualenv --python=/sw/dsi/aarch64/centos7/python/3.7.4/bin/python3 venv
venv/bin/python -m pip install -r requirements.txt