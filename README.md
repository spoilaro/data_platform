# Data Platform

## Project Description
A platform to collect, store and visualize logs. Written in Rust and Python.

## Instructions

```
# Log Warehouse
# NOTE! Run this first since the warehouse acts as TCP server
cargo run --bin warehouse

# Log Collector
cargo run --bin collector

# Visualizer

## Creates the environment and activates it
python3 -m venv env
source ./env/bin/activate

## Install the dependacies
pip install -r requirements.txt

## Run the visualizer server
 python3 viz/main.py
```


