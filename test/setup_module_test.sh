#!/bin/bash

set -e
set -xv

# Current directory
cur_dir=$PWD
echo "Current directory: '$cur_dir' ..."

# Python version
py_version_short=$(python -c "import sys; print(''.join(str(x) for x in sys.version_info[:2]))")
# -> 27 or 34 or ..
echo "Python version: $py_version_short"

get_name (){
    echo $(python -c 'import json; print json.load(open("'$1'package.json"))["name"]')
}

setup_submodule (){
   local dep
   local mname
   local mpath
   for dep in $(cat test/dep_modules.txt); do
      # Module directory
      mname=$(basename $dep | sed 's/.git//g')
      mpath="test/tmp/$mname"

      # Module repo
      if [ -d "$mpath" ]
      then
         echo "Module $mpath is still cloned"
      else
         git clone --depth 10  "$dep" "$mpath"
      fi
      ( cd "$mpath" && git status && git log -1)

      # Map module directory to the Shinken test modules directory
      rmname=$(get_name "$mpath/")
      if [ ! -d "$PWD/$SHI_DST/test/modules/$rmname" ]
      then
         ln -s "$PWD/$mpath/module" "$PWD/$SHI_DST/test/modules/$rmname"
      fi

      if [ -f "$PWD/$mpath/test/mock_livestatus.py" ]
      then
         if [ ! -f "$PWD/$SHI_DST/test/modules/$rmname/mock_livestatus.py" ]
         then
            ln -s "$PWD/$mpath/test/mock_livestatus.py" "$PWD/$SHI_DST/test/modules/$rmname/mock_livestatus.py"
         fi
      fi
      # Extend the test configurations with the modules one
      if [ -d "$PWD/$mpath/test/etc" ]
      then
         cp -r "$PWD/$mpath/test/etc" "$PWD/$SHI_DST/test"
      fi

      # Install the modules Python requirements
      if [ -f "$mpath/requirements.txt" ]
      then
         pip install -r "$mpath/requirements.txt"
      fi
      if [ -f "$mpath/requirements.py${py_version_short}.txt" ]
      then
         pip install -r "$mpath/requirements.py${py_version_short}.txt"
      fi
   done
}

name=$(get_name)

#rm -rf test/tmp
#mkdir -p test/tmp/

# Clone and configure Shinken
SHI_DST=test/tmp/shinken
# Extend the test configurations with the modules one
if [ -d "$SHI_DST" ]
then
   echo "Shinken is still cloned"
else
   git clone --depth 10 https://github.com/naparuba/shinken.git "$SHI_DST"
fi
( cd "$SHI_DST" && git status && git log -1)

echo 'Installing Shinken tests requirements...'
(
    cd "$SHI_DST"
    pip install -r test/requirements.txt
    if [ -f "test/${spec_requirement}" ]
    then
        pip install -r "test/${spec_requirement}"
    fi
)

echo 'Installing tests requirements + application requirements...'
pip install --upgrade -r test/requirements.txt
if [ -f "test/requirements.py${py_version_short}.txt" ]
then
    pip install -r "test/requirements.py${py_version_short}.txt"
fi

# Map module directory to the Shinken test modules directory
rmname=$(get_name "$mpath/")
if [ ! -d "$SHI_DST/test/modules/$name" ]
then
   ln -s "$PWD/module" "$SHI_DST/test/modules/$name"
fi

# Install the necessary sub-modules
if [ -f test/dep_modules.txt ]
then
    setup_submodule
fi
echo "Python path '$PYTHONPATH' ..."
