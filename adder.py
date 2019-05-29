#!/usr/bin/python3
# -*- coding: latin-1 -*-

import requests, ast, csv, sys, os
import wikidataintegrator as WI

"""
abgeändert von: "https://github.com/SemanticLab/data-2-wikibase/blob/master/add_items.py"
In Arbeit
erhält .csv file als Parameter und fügt alle Spalten zum Wiki (localhost) hinzu.
csv Tabelle muss Zeile "Label" haben, erste Zeile sind die Keys [Form-Beispiel: "Label,P2:string,P4:item"]
Properties müssen im Wiki bereits vorhanden sein.
Benötigt eine Datei "password" entweder im gleichen Ordner oder als Pfad mit Parameter "pw:PFAD".
Die Passwort-Datei muss einen Tupel für den Bot-Login [z.B.: ("Admin", "bot@od9duv8log5tsduq17sa2c86iffnf5t3")] enthalten.

Nach erfolgreichem beenden des skripts sollten 2 Dateien entstehen:
"csv_path_updated.csv" und "csv_path_errors.csv" wobei csv_path dem ursprünglichen namen der csv-Datei entspricht
"csv_path_updated.csv" enthält alle erfolgreich hinzugefügten Items mit neu erstellter QID
"csv_path_errors.csv" enthält alle Items bei denen das Hinzufügen fehlgeschlagen ist
"""

sparql_endpoint_url = "http://localhost:8282/proxy/wdqs/bigdata/namespace/wdq/sparql"
mediawiki_api_url = "http://localhost:8181/w/api.php"

if __name__ == "__main__":
    if len(sys.argv)<2:
        raise ValueError('No input-file parameter')
    else:
        csv_path = str(sys.argv[1])
        if not os.path.isfile(csv_path):
            raise ValueError('Input Parameter: '+csv_path+" is no valid file.")
        pw = []
        if len(sys.argv)>2:
            param = sys.argv[2:]
            pw = [o[3:] for o in param if o.startswith("pw:")]
        password_file = "password" if len(pw)==0 else pw[-1]

    print(password_file)

    csv_file = open(csv_path,"r",encoding="UTF-8",errors="ignore")
    csv_dict = csv.DictReader(csv_file)
    with open(password_file,"r",encoding="UTF-8",errors="ignore") as f:
        data = f.readline().strip()
    name,pw = ast.literal_eval(data)

    login_instance = WI.wdi_login.WDLogin(user=name, pwd=pw, mediawiki_api_url=mediawiki_api_url)

    complete_data = []
    errors_data = []

    for row in csv_dict:
        row = dict(row)
        data = []
        err = False
        if not "Label" in row:
            err = True
        try:
            for key in row:
                if key != None and len(key)>0 and ":" in key and key.startswith("P"):
                    z = key.find(":")
                    type = key[z+1:]
                    if type == "string":
                        data.append(WI.wdi_core.WDString(value=row[key], prop_nr=key[:z]))
                    elif type == "item":
                        data.append(WI.wdi_core.WDItemID(value=row[key], prop_nr=key[:z]))
                    elif type == "multiple-items":
                        for i in row[key].split(";"):
                            if len(i)>1 and i.startswith("Q") and i[1:].isdigit():
                                data.append(WI.wdi_core.WDItemID(value=i, prop_nr=key[:z]))
                            else:
                                err = True
                                row["error"] = "Error adding multiple items"
        except Exception as e:
            print("There was an error with this one, skipping:")
            print(row)
            print(e)
            err = True
            row["error"] = "error adding statements: "+str(e)

        try:
            if not err:
                wd_item = WI.wdi_core.WDItemEngine(data=data, new_item=True, mediawiki_api_url=mediawiki_api_url, sparql_endpoint_url=sparql_endpoint_url)
                wd_item.set_label(row["Label"])
                if "Description" in row:
                    wd_item.set_description(row["Description"])

                r = wd_item.write(login_instance)

                row["QID"] = r
                print("ADDED: "+str(row["Label"])+" as: "+str(r))

        except Exception as e:
            print("Skipping this one...")
            print("Error:",e)
            err = True
            row["error"] = "error while adding the item: "+str(e)

        if err:
            errors_data.append(row)
        else:
            complete_data.append(row)

    csv_file.close()
    csv_base = csv_path.rsplit(".",1)[0]
    if len(complete_data) > 0:
        with open(csv_base+'_updated.csv','w') as out:

            fieldnames = list(complete_data[0].keys())
            writer = csv.DictWriter(out, fieldnames=fieldnames)

            writer.writeheader()
            writer.writerows(complete_data)


    if len(errors_data) > 0:
        with open(csv_base+'_errors.csv','w') as out:

            fieldnames = list(errors_data[0].keys())
            writer = csv.DictWriter(out, fieldnames=fieldnames)

            writer.writeheader()
            writer.writerows(errors_data)
