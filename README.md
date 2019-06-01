# data-mapping
tools to add data to the wiki

### Requirements
Python 3.7.x
Git

### Try it out
Make sure you have a clean WikiBase instance, no properties or items should be there.

To run, simply execute:
```bash
./import.sh
```

The script will automatically download the SPARQL ready data from `https://github.com/code-openness/Data.git` and import it.

NOTE: this script assumes all the data is new and does not exist in the database.
