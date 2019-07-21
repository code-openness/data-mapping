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

"""
Read the Documentation on github for more explanation.
https://code-openness.github.io/
"""


"""
* saves a dictionary to a file
json_file:  String of file path
dict:       Dictionary that will be saved
"""
def save_json(json_file, dict):
    data = json.dumps(dict)
    with open(json_file, "w") as f:
        f.write(data)
    print("[Wrote JSON] "+json_file)

"""
* loads a dictionary from a file
json_file:  String of file that is loaded
return:=    dictionary of file content
"""
def load_json(json_file):
    if not os.path.isfile(json_file):
        return {}
    with open(json_file, "r") as f:
        data = f.read()
    return json.loads(data)

"""
* imports properties into wikibase
site:               pywikibot.Site of the wikibase
load_from_csv_file: String of file path where properties are imported from
save_csv_file:      String of file path where csv with resulting PID is saved
save_prop_map_json: String of file path where properties are stored as dictionary
return:=            dictionary that maps local property id to PID
"""
def import_properties(site, load_from_csv_file='./data/properties.csv',
        save_csv_file=None, save_prop_map_json="./data/prop_map.json"):
    if save_csv_file==None:
        save_csv_file = load_from_csv_file
    prop_map = {}
    props = pd.read_csv(load_from_csv_file, dtype=object)

    props.insert(0,"PID","")
    for _, row in props.iterrows():
        datatype = row['data_type']
        label = row['label']
        data = {
            'datatype': datatype,
            'labels': {
                'en': {
                    'language': 'en',
                    'value': label
                }
            }
        }
        params = {
            'action': 'wbeditentity',
            'new': 'property',
            'data': json.dumps(data),
            'summary': 'bot adding in properties',
            'token': site.tokens['edit']
        }
        req = site._simple_request(**params)
        results = req.submit()

        PID = results['entity']['id']
        row["PID"] = PID
        prop_map[row['id']] = {'datatype': datatype, 'PID': PID}
        print(" "*(14-len(PID)) + PID + " : " + label)

    props.to_csv(save_csv_file, index=False)
    save_json(save_prop_map_json, prop_map)
    return prop_map

"""
* writes a WDItemEngine to wikibase
wd_item:                    WDItemEngine that is to be written
label:                      label of the item (for printing result in console only)
item_map:                   dictionary that maps local item ids to QIDs
login_instance:             WDLogin instance of wikidataintegrator
local_id:                   local item id that is to be mapped to its QID
standard_QID_output_file:   String of file path where results are logged
return:=                    QID
"""
def write_item(wd_item, label, login_instance, item_map={}, local_id=None,
        standard_QID_output_file=None):
    QID = wd_item.write(login_instance)
    if not local_id == None:
        item_map[local_id] = QID # add QID to the item_map
    print("[Item added] "+ QID +" "*(14-len(QID)) + " : " + label) # print QID and label
    if not standard_QID_output_file==None:
        if os.path.isfile(standard_QID_output_file) or os.path.isdir(os.path.dirname(standard_QID_output_file)):
            with open(standard_QID_output_file, "a") as f:
                f.write(QID+","+local_id+","+label+"\n") # logs the result
    return QID

"""
* imports items from csv file
csv_file:           String of file path where items should be imported from
prop_map:           dictionary that maps local property ids to dictionary with keys {datatype, PIDs}
item_map:           dictionary that maps local item ids to QIDs
login_instance:     WDLogin instance of wikidataintegrator
MEDIA_WIKI_API:     String of wiki api url
SPARQL_ENDPOINT:    String of sparql endpoint url
"""
def import_items_from_file(csv_file, prop_map, item_map, login_instance,
        MEDIA_WIKI_API="http://localhost:8181/w/api.php",
        SPARQL_ENDPOINT="http://localhost:8282/proxy/wdqs/bigdata/namespace/wdq/sparql",):
    items = pd.read_csv(csv_file, dtype=object)
    print("[Import started] Started import for file: "+csv_file+" with "+str(items.shape[0])+" items")
    local_prop_ids =    list(items.columns)[2:]  # ignore the columns id and label
    for prop in local_prop_ids:
        if prop not in prop_map:
            print("[Property unavailable] Property '" +prop+ "' not in prop_map, skipped for all items.")
            print(prop_map)
            continue

    items.insert(0,"QID","")
    for index, row in items.iterrows():
        label = row['label']
        if label is np.nan:
            print("[No Item Label] Ignoreing elment with no label, index: "+str(index))
            continue

        data = [] # statements to be given to wd_item
        for prop in local_prop_ids:
            if prop not in prop_map:
                continue
            value = row[prop]
            if value is np.nan:
                print("[Empty Property Value] Value of Property '" +prop+ "' is empty, skipping for item: "+str(index)+": "+label)
                continue
            prop_type = prop_map[prop]["datatype"]
            prop_PID  = prop_map[prop]["PID"]
            if prop_type == "wikibase-item":
                # we split because it can be multiple values, separated by semicolon
                values = [val.strip() for val in value.split(";")]
                # map from local ids to QIDs
                values = list(map(lambda _id: item_map[_id], values))
                # create statments for these values
                statements = list(map(lambda val: WI.wdi_core.WDItemID(value=val, prop_nr=prop_PID), values))
                # add to data
                data.extend(statements)
            elif prop_type == "string":
                data.append(WI.wdi_core.WDString(value=value, prop_nr=prop_PID))
            else:
                print("[Undefined Property-type] Undefined action for property-type: "+ prop_type +" with value "+ value +"in item: "+ str(index) +": "+ label)
                continue
            # add other property-types here

        wd_item = WI.wdi_core.WDItemEngine(
            data=data,
            mediawiki_api_url=MEDIA_WIKI_API,
            sparql_endpoint_url=SPARQL_ENDPOINT
            #core_props="P1", new_item=True # leave commented, for testing only
        )
        wd_item.set_label(label)
        row["QID"] = write_item(wd_item, label, item_map, login_instance, local_id=row["id"])

    save_to = os.path.dirname(csv_file) + "/" + "saved_" + os.path.basename(csv_file) # change saved file path here
    items.to_csv(save_to, index=False)
    print("[Saved file] "+save_to)


if __name__ == "__main__":
    param = ""
    if len(sys.argv)>1:
        param = sys.argv[1]
    from parameters import BOT_USERNAME, BOT_PASSWORD, MEDIA_WIKI_API, MEDIA_WIKI_SERVER, SPARQL_ENDPOINT

    to_import = "./data/"
    property_file = './data/properties.csv'
    item_map = {}
    def wait(self, secs):# to overwrite the wait function
        pass
    pywikibot.throttle.Throttle.wait = wait
    site = pywikibot.Site('wikidata', 'wikidata')
    site.login()

    if not os.path.isdir(to_import):
        raise ValueError(to_import + "is not a valid directory") # check if data/properties.csv exists
    if not os.path.isfile(property_file):
        raise ValueError(property_file + "is not a valid file") # check if data/properties.csv exists

    item_files = sorted(glob.glob(to_import+"items_*.csv")) # all csv-files to be imported, sorted

    if "-noprop" in param: # if -noprop in parameter 1 then load property from json file, when properties in wikibase but no items
        prop_map = load_json("./data/prop_map.json")
    else:
        prop_map = import_properties(site)

    login_instance = WI.wdi_login.WDLogin(
        user=BOT_USERNAME,
        pwd=BOT_PASSWORD,
        mediawiki_api_url=MEDIA_WIKI_API
    )
    for file in item_files:
        import_items_from_file(file, prop_map, item_map, login_instance)
        save_json("./data/item_map.json", item_map)
    save_json("./data/item_map.json", item_map)
    save_json("./data/prop_map.json", prop_map)
    print("[Import done]")
