#!/usr/bin/env bash
set -e
# write bot name and pasword here, no quotaions " or '
export BOT_USERNAME=pik
export BOT_PASSWORD=bot@rmj5c9dhv4k5gojmpk3bt4i064qj1ca9

echo "('$BOT_USERNAME','$BOT_PASSWORD')" > password
echo "{\"username\":\"$BOT_USERNAME\", \"password\":\"$BOT_PASSWORD\"}" > password.json

python3 -m venv import_env
source import_env/bin/activate
python3 -m pip install --upgrade pip
python3 -m pip install  -r requirements.txt

git clone -b sparql --single-branch https://github.com/code-openness/Data.git cloned

rm -rf data
mkdir -p data
cp -r ./cloned/raw/sparql/*.csv ./data
rm -r -f ./cloned

python3 import.py

deactivate
