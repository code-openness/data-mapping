import sys, os, csv

"""
erhält 4 Parameter:
1: csv-file with keys/values
2. csv-file where key should be replaced with value
3. key:KEY      KEY must be column name in csv-file from param 1
3. value:VALUE  VALUE must be column name in csv-file from param 1

"""

if __name__ == "__main__":
    if len(sys.argv)<5:
        raise ValueError('4 Parameters needed')
    else:
        path_1,path_2 = str(sys.argv[1]),str(sys.argv[2])
        if not os.path.isfile(path_1) or not os.path.isfile(path_2):
            raise ValueError('Parameter 1 or 2 is no file.')
        param = sys.argv[3:]
        key_list = [tuple(o.split(":",1)) for o in param if ":" in o and o.find(":")>0 and o.find(":")<len(o)-1]
        ls = [o for o in key_list if o[0].lower() == "key"]
        if len(ls)<1:
            raise ValueError('Parameter  key:"KEY"  needed')
        else:
            key = ls[0][1]
        xs = [o for o in key_list if o[0].lower() == "value"]
        if len(ls)<1:
            raise ValueError('Parameter  value:"VALUE"  needed')
        else:
            value = xs[0][1]
        # alles vor dieser Zeile nur um Parameter zu empfangen

    csv_file = open(path_1,"r",encoding="UTF-8",errors="ignore")

    csv_dict = csv.DictReader(csv_file)
    replace_file = open(path_2,"r",encoding="UTF-8",errors="ignore")
    data = replace_file.read()
    replace_file.close()
    replace_file = open(os.path.basename(path_2)+"_updated.csv","w",encoding="UTF-8",errors="ignore")

    rows = []
    for row in csv_dict:
        rows.append(row) # weil DictReader nur einmal auslesbar ist
    if len(rows)<1:
        raise ValueError(path_1 + " contains no valid csv format/is empty")

    if not key in rows[0] or not value in rows[0]:
        raise ValueError(path_1 + " does not have keys: "+key+", "+value)

    dict = dict()

    for row in rows:
        dict[row[key]] = row[value]

    for key in dict:
        data = data.replace(key+";", '"'+dict[key]+'";')
        data = data.replace(key+",", '"'+dict[key]+'",')

    replace_file.write(data)
    replace_file.close()
