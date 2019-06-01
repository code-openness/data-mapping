# data-mapping
tools to add data to the wiki

### Requirements
Python 3.7.x
Git

### Try it out
Make sure you have a clean WikiBase instance, no properties or items should be there.

Write your bot's username and password in the `import.sh` file, and then simply execute the script:
```bash
./import.sh
```

The script will automatically download and install the dependencies and also download the SPARQL ready data from `https://github.com/code-openness/Data.git` and import it.

NOTE: this script assumes all the data is new and does not exist in the database.
