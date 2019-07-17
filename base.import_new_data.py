import pywikibot
import json
import pandas as pd
import numpy as np
import glob
import requests
import re
import wikidataintegrator as WI
import sys, os
from pathlib import Path
from parameters import BOT_USERNAME, BOT_PASSWORD, MEDIA_WIKI_API, MEDIA_WIKI_SERVER, SPARQL_ENDPOINT

if __name__ == "__main__":
    if len(sys.argv)<4:
        if len(sys.argv)==2 and (sys.argv[1].startswith("--help") or sys.argv[1].startswith("-h")):
            print("Start this program with 2 Parameters:")
            print("Parameter 1: prop_map.json  /  the file in which the prop_map is stored")
            print("Parameter 2: item_map.json  /  the file in which the item_map is stored")
            print("Parameter 3: items.csv  /  the file in which the items to be imported are stored")
            quit()
        raise ValueError("You have to give 2 Parameters")
    for i in range(1,4):
        if not os.path.isfile(sys.argv[i]):
            raise ValueError("Parameter "+sys.argv[i]+" is not a valid file")
    prop_map = load_json(sys.argv[1])
    item_map = load_json(sys.argv[2])
    login_instance = WI.wdi_login.WDLogin(user=BOT_USERNAME, pwd=BOT_PASSWORD, mediawiki_api_url=MEDIA_WIKI_API)
    import_items_from_file(sys.argv[3], prop_map, item_map, login_instance)
    save_json("./data/item_map.json", item_map)
    print("import done")
