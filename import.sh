#!/usr/bin/env bash
set -e

# write bot name and pasword here, no quotaions " or '
export USERNAME=pik
export BOT_PASSWORD=password123
export MEDIA_WIKI_SERVER=http://localhost:8181
export MEDIA_WIKI_API=$MEDIA_WIKI_SERVER/w/api.php
export SPARQL_ENDPOINT=http://localhost:8282/proxy/wdqs/bigdata/namespace/wdq/sparql

echo "Importing using the username: $BOT_USERNAME"
echo "WARN: This script only work with a clean wikibase, ot properties or items should be in the database"

echo "('$BOT_USERNAME','$BOT_PASSWORD')" > password
envsubst < base.import.py > import.py
envsubst < base.user-config.py > user-config.py

python3 -m venv import_env
source import_env/bin/activate
python3 -m pip install --upgrade pip
python3 -m pip install  -r requirements.txt

git clone -b sparql_2 --single-branch https://github.com/code-openness/Data.git cloned

mkdir -p data
cp -r ./cloned/sparql/sparql/*.csv ./data
rm -r -f ./cloned

python3 import.py

deactivate
