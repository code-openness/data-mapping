import pywikibot
import json
import pandas as pd
import numpy as np
import glob
import requests
import re
import wikidataintegrator as WI


# overwrite the wait function
def wait(self, secs):
    pass


pywikibot.throttle.Throttle.wait = wait
site = pywikibot.Site('wikidata', 'wikidata')
site.login()

# import properties
props = pd.read_csv('data/properties.csv')
# add empty column for the new PID
props.loc[:, 'PID'] = pd.Series([None] * len(props.index), index=props.index)

prop_map = {}
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
    row['PID'] = PID
    prop_map[row['id']] = {'datatype': datatype, 'PID': PID}
    print(PID, label)

props.to_csv('data/saved_properties.csv', index=False)
print("saved properties")
# import items

# setup wikidata integrator
MEDIA_WIKI_API = 'http://localhost:8181/w/api.php'
SPARQL_ENDPOINT = 'http://localhost:8282/proxy/wdqs/bigdata/namespace/wdq/sparql'

#read password
password_file = open("./password.json", "r")
auth_dict = json.load(password_file)
password_file.close()

# setup login
login_instance = WI.wdi_login.WDLogin(
    user=auth_dict['username'], 
    pwd=auth_dict['password'],
    mediawiki_api_url=MEDIA_WIKI_API
)

# helper
def is_nan(x):
    return (x is np.nan or x != x)

item_files = glob.glob('./data/items_*.csv')
item_map = {}

for fileIndex in range(len(item_files)):
    fileName = './data/items_' + str(fileIndex) + '.csv'
    print("processing file ", fileName)
    items = pd.read_csv(fileName)

    local_prop_ids = list(items.columns)[2:]  # ignore the columns id and label
    prop_types = list(map(lambda local_id: prop_map[local_id]['datatype'], local_prop_ids))
    PIDs = list(map(lambda local_id: prop_map[local_id]['PID'], local_prop_ids))
    #continue

    # add empty column for the new QID
    items.loc[:, 'QID'] = pd.Series([None] * len(items.index), index=items.index)

    for index, row in items.iterrows():
        data = []
        for prop_index, local_prop_id in enumerate(local_prop_ids):
            value = row[local_prop_id]
            if is_nan(value):
                continue
            
            prop_type = prop_types[prop_index]
            PID = PIDs[prop_index]
            if prop_type == 'wikibase-item':
                # we split because it can be multiple values
                values = [val.strip() for val in value.split(";")]
                # map from local ids to QIDs
                values = list(map(lambda _id: item_map[_id], values))
                # create statments for these values
                statements = list(map(lambda val: WI.wdi_core.WDItemID(value=val, prop_nr=PID), values))
                # add to data
                data = data + statements
            elif prop_type == 'string':
                data.append(WI.wdi_core.WDString(value=value, prop_nr=PID))
            else:
                print("Ignoring unknown type:", prop_type, "with value", value) # add other types here
        # this is copied from the turorial, the api is now a bit different
        wd_item = WI.wdi_core.WDItemEngine(
            data=data, 
            mediawiki_api_url=MEDIA_WIKI_API,
            sparql_endpoint_url=SPARQL_ENDPOINT
        )
        label = row['label']
        wd_item.set_label(label)
        # write to database
        QID = wd_item.write(login_instance)
        # save new QID to file and map
        row['QID'] = QID
        local_id = row['id']
        item_map[local_id] = QID
        print(QID, label)

    # after import is done, write back to new file
    items.to_csv('./data/saved_items_' + str(fileIndex) + '.csv', index=False)
    print("saved file ", './data/saved_items_' + str(fileIndex) + '.csv')




