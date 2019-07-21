#!/usr/bin/env bash
set -e

# you should execute import.sh before executing this,
# so that you have the proper directory structure and all needed files
NEW_FILE="./data/items_0.csv"
ITEM_MAP="./data/item_map.json"
PROP_MAP="./data/prop_map.json"

source import_env/bin/activate
python3 base_import_new_data.py "$PROP_MAP" "$ITEM_MAP" "$NEW_FILE"
deactivate
