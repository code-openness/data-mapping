import pywikibot
import json
import pandas as pd
import numpy as np
import glob
import requests
import re
import wikidataintegrator as WI
import sys
from pathlib import Path


# overwrite the wait function
def wait(self, secs):
    pass

pywikibot.throttle.Throttle.wait = wait
site = pywikibot.Site('wikidata', 'wikidata')
site.login()

# import properties
def import_properties(load_from_csv_file='./data/properties.csv',save_csv_file='./data/saved_properties.csv', save_prop_map_json="./data/prop_map.json"):
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

def save_json(json_file, dict):
    data = json.dumps(dict)
    with open(json_file, "w") as f:
        f.write(data)

def load_json(json_file):
    if not os.path.isfile(json_file):
        return {}
    with open(json_file, "w") as f:
        data = f.read()
    return json.loads(data)

# helperfunction
def is_nan(x):
    return (x is np.nan or x != x)

def write_item(wd_item, label, item_map, login_instance, local_id=None, standard_QID_output_file='./data/saved_items.csv'):
    QID = wd_item.write(login_instance)
    if not local_id == None:
        item_map[local_id] = QID # add QID to the item_map
    print(" "*(14-len(QID)) + QID + " : " + label) # print QID and label
    with open(standard_QID_output_file, "a") as f:
        f.write(QID+","+local_id+","+label+"\n") # append line to standard_QID_output_file
    return QID

def import_items_from_file(csv_file, prop_map, item_map, login_instance,
        MEDIA_WIKI_API="http://localhost:8181/w/api.php", SPARQL_ENDPOINT="http://localhost:8282/proxy/wdqs/bigdata/namespace/wdq/sparql",):
    items = pd.read_csv(csv_file, dtype=object)
    local_prop_ids =    list(items.columns)[2:]  # ignore the columns id and label

    items.insert(0,"QID","")
    for index, row in items.iterrows():
        label = row['label']
        if label is np.nan:
            print("Ignoreing elment with no label, ", index)
            continue

        data = [] # statements to be given to wd_item
        for prop in local_prop_ids:
            if prop not in prop_map:
                print("Property '" +prop+ "' not in prop_map, skipping property for item: "+label)
                continue
            value = row[prop]
            if value is np.nan:
                print("Value of Property '" +prop+ "' is empty, skipping for item: "+label)
                continue
            prop_type = prop["datatype"]
            prop_PID  = prop["PID"]
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
                print("Ignoring unknown type:", prop_type, "with value", value)
                continue
            # add other property-types here

        wd_item = WI.wdi_core.WDItemEngine(
            data=data,
            mediawiki_api_url=MEDIA_WIKI_API,
            sparql_endpoint_url=SPARQL_ENDPOINT
        )
        wd_item.set_label(label)
        row["QID"] = write_item(wd_item, label, item_map, login_instance, local_id=row["id"])

    save_to = os.path.dirname(csv_file) + "/" + "saved_" + os.path.basename(csv_file) # change saved file path here
    items.to_csv(save_to, index=False)
    print("Saved file: "+save_to)


if __name__ == "__main__":
    from parameters import BOT_USERNAME, BOT_PASSWORD, MEDIA_WIKI_API, MEDIA_WIKI_SERVER, SPARQL_ENDPOINT
    to_import = "./data/"
    if not os.path.exists(to_import):
        os.makedirs(to_import)
    property_file = './data/properties.csv'
    if not os.path.isfile(property_file):
        raise ValueError(property_file + "is not a valid file")
    if os.path.isdir(to_import):
        item_files = [os.dirname(to_import)+"/"+o for o in os.listdir(to_import) if o.endswith(".csv")] # all csv-files to be imported
    item_map = {}
    prop_map = import_properties()
    login_instance = WI.wdi_login.WDLogin(
        user='BOT_USERNAME',
        pwd='BOT_PASSWORD',
        mediawiki_api_url=MEDIA_WIKI_API
    )
    for file in item_files:
        import_items_from_file(file, prop_map, item_map, login_instance)
    save_json("./data/item_map.json", item_map)
    save_json("./data/prop_map.json", prop_map)
    print("import done")
