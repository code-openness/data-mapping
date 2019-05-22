import requests, ast, csv, sys
import wikidataintegrator as WI

"""
In Arbeit
erhält .csv file als Parameter und fügt alle Spalten zum Wiki (localhost) hinzu.
csv Tabelle muss Zeilen "Label" und "Description" haben, erste Zeile sind die Keys/Properties [Form: "P2:string" oder "P4:item"]
Properties müssen im Wiki bereits vorhanden sein.
Benötigt eine Datei "password" im gleichen Ordner mit einem Tupel für den Bot-Login [z.B.: ("Admin", "bot@od9duv8log5tsduq17sa2c86iffnf5t3")]

es felht:
- hinzufügen von wikibase-item properties [bisher nur strings]
- error beim hinzufügen von "Malformed input" [z.b. manche Sonderzeichen in Strings]
- hinzufügen von mehreren einträgen der gleichen property [z.b. mehrere Authoren des gleichen Papers]
"""

if __name__ == "__main__":
    if len(sys.argv)<2:
        raise ValueError('No input-file parameter')

    file = open(sys.argv[1],"r",encoding="UTF-8",errors="ignore")
    csv_dict = csv.DictReader(file)

    with open("password","r",encoding="UTF-8",errors="ignore") as f:
        data = f.readline().strip()
    name,pw = ast.literal_eval(data)

    login_instance = WI.wdi_login.WDLogin(user=name, pwd=pw, mediawiki_api_url='http://localhost:8181/w/api.php')

    for row in csv_dict:

        row = dict(row)
        data = []
        for key in row:
            if key != None and ":" in key:
                data.append(WI.wdi_core.WDString(value=row[key], prop_nr=key.split(":")[0]))

        wd_item = WI.wdi_core.WDItemEngine(core_props="P5",data=data, new_item=True, mediawiki_api_url='http://localhost:8181/w/api.php',sparql_endpoint_url="http://localhost:8282/proxy/wdqs/bigdata/namespace/wdq/sparql")
        wd_item.set_label(row["Label"])
        wd_item.set_description(row["Description"])
        try:
            r = wd_item.write(login_instance)
        except Exception as e:
            print("Error:",e)
        print("ADDED: "+str(row["Label"]))
