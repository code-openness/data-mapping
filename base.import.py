import pywikibot
import json
import pandas as pd
import numpy as np
import glob
import requests
import re
import wikidataintegrator as WI
import sys
import threading
import time
from pathlib import Path

max_threads = 30
# overwrite the wait function
def wait(self, secs):
    pass
"""
prop_map = {}
props = pd.read_csv('./data/saved_properties.csv')
for _, row in props.iterrows():
    prop_map[row['id']]={
       'PID': row["PID"],
       'datatype': row['data_type']}


"""
pywikibot.throttle.Throttle.wait = wait
site = pywikibot.Site('wikidata', 'wikidata')
site.login()

# import properties

prop_map = {}
props = pd.read_csv('./data/properties.csv')

new_PIDS = []
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
    new_PIDS.append(PID)
    prop_map[row['id']] = {'datatype': datatype, 'PID': PID}
    print(PID, label)


props['PID'] = pd.Series(new_PIDS, index=props.index)
props.to_csv('./data/saved_properties.csv', index=False)
print("saved properties")
print("imported props", prop_map)
# import items


# setup wikidata integrator
MEDIA_WIKI_API = 'http://localhost:8181/w/api.php'
SPARQL_ENDPOINT = 'http://localhost:8282/proxy/wdqs/bigdata/namespace/wdq/sparql'

login_instance = WI.wdi_login.WDLogin(
    user='Pik',
    pwd='$BOT_PASSWORD',
    mediawiki_api_url=MEDIA_WIKI_API
)

# helper
def is_nan(x):
    return (x is np.nan or x != x)

item_files = glob.glob('./data/items_*.csv')
item_map = {}
DONE, all_run = False, False

for fileIndex in range(len(item_files)):
    fileName = './data/items_' + str(fileIndex) + '.csv'
    print("processing file ", fileName)
    items = pd.read_csv(fileName)

    local_prop_ids = list(items.columns)[2:]  # ignore the columns id and label
    prop_types = list(map(lambda local_id: prop_map[local_id]['datatype'], local_prop_ids))
    PIDs = list(map(lambda local_id: prop_map[local_id]['PID'], local_prop_ids))


    threads = []
    new_data = []

    DONE, all_run = False, False
    def bg_thread():
        global DONE, all_run
        i = 0
        all_run = False
        while not all_run:
            alive = len([o for o in threads if o.is_alive()])
            while alive > 30:
                time.sleep(0.1)
            if i+1<=len(threads):
                threads[i].start()
                i+=1
            else:
                time.sleep(0.05)
            if DONE and i+1>=len(threads) and alive == 0:
                all_run=True

    bg_t = threading.Thread(target=bg_thread)
    bg_t.daemon = True # daemon thread beendet wenn programm beendet ist
    bg_t.start()

    for index, row in items.iterrows():
        label = row['label']
        if is_nan(label):
            print("Ignoreing elment with no label, ", index)
            continue

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
                data.extend(statements)
            elif prop_type == 'string':
                data.append(WI.wdi_core.WDString(value=value, prop_nr=PID))
            else:
                print("Ignoring unknown type:", prop_type, "with value", value)
                # add other types here

        wd_item = WI.wdi_core.WDItemEngine(
            data=data,
            mediawiki_api_url=MEDIA_WIKI_API,
            sparql_endpoint_url=SPARQL_ENDPOINT,
        )

        wd_item.set_label(label)
        local_id = row['id']

        def thread_function(_wd_item, _index, _local_id, _new_data):
            global item_map
            QID = _wd_item.write(login_instance)
            new_data.append([_index, _local_id, _QID])
            item_map[_local_id] = QID
            print("Added: "+str(QID)+" |",_local_id)

        t = threading.Thread(target=thread_function,args=[wd_item, index, local_id, new_data])
        threads.append(t)
        print("threads append: ", local_id)

    DONE = True # alle threads wurden in die liste threads geschrieben
    while not all_run: #bg_thread setzt all_run auf True wenn alle Threads ausgeführt wurden
        time.sleep(0.1)

    new_data.sort(key=lambda x: x[0])
    for _id, _local_id, _QID in new_data:
        item_map[_local_id] = _QID
    new_data = pd.DataFrame(new_data, columns=['i', 'local_id', 'QID'])
    # after import is done, write back to new file
    items['QID'] = new_data['QID']
    #items['QID'] = pd.Series(new_QIDS, index=items.index)
    items.to_csv('./data/saved_items_' + str(fileIndex) + '.csv', index=False)
    print("saved file ", './data/saved_items_' + str(fileIndex) + '.csv')
print("import done")
