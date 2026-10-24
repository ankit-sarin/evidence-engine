#!/bin/bash
cd $HOME/projects/evidence-engine
export U17_CENSUS_OUT=/home/ankitsarin/scratch/12w-u17b/census/census.jsonl PYTHONPATH=/home/ankitsarin/scratch/12w-u17b/census
date -u +%FT%T.%NZ > /home/ankitsarin/scratch/12w-u17b/census/census_launch.txt
.venv/bin/python -m pytest tests/ -q -p u17_census_plugin -p no:cacheprovider --basetemp=/home/ankitsarin/scratch/12w-u17b/census/basetemp -m "not network and not ollama and not integration" > /home/ankitsarin/scratch/12w-u17b/census/census_pytest.log 2>&1; echo $? > /home/ankitsarin/scratch/12w-u17b/census/census_rc.txt
date -u +%FT%T.%NZ > /home/ankitsarin/scratch/12w-u17b/census/census_exit.txt
