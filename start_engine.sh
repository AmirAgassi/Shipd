#!/bin/bash
sudo -v

sudo lsof -t -i:9000 | xargs sudo kill -9
sudo lsof -t -i:9001 | xargs sudo kill -9
sudo lsof -t -i:9002 | xargs sudo kill -9

sudo python3 source_code/engine_source_code/engine.py 9000 &
sudo python3 source_code/engine_source_code/engine.py 9001 &
sudo python3 source_code/engine_source_code/engine.py 9002 &