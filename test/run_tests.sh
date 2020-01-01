#!/usr/bin/env bash

# Unit tests
cur_dir=$PWD

echo "Current directory: '$cur_dir' ..."
export PYTHONPATH=$PWD
export PYTHONPATH=$PYTHONPATH:$PWD/test/tmp/shinken # we also need shinken test/modules...
export PYTHONPATH=$PYTHONPATH:$PWD/test/tmp/shinken/test # we also need shinken test/modules...
export PYTHONPATH=$PYTHONPATH:$PWD/test/tmp/shinken/test/modules # we also need mock_livestatus from mod-livestatus..
export PYTHONPATH=$PYTHONPATH:$PWD/test/tmp/shinken/test/modules/livestatus # we also need mock_livestatus from mod-livestatus..
echo "Python path: '$PYTHONPATH' ..."

cd "$cur_dir"/test/tmp/shinken/test
#pytest -vv --durations=0 --no-print-logs --cov="$cur_dir"/module --cov-config "$cur_dir"/test/.coveragerc "$cur_dir"/test/test_*.py
pytest -vv --durations=0 --no-print-logs --cov="$cur_dir"/module --cov-report=xml --cov-config "$cur_dir"/test/.coveragerc "$cur_dir"/test/test_*.py

cd "$cur_dir"/test
