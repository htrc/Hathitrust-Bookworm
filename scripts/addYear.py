import os, sys, json

#The catalog metadata was built without year data. We can't rebuild the catalog from scratch because that would re-assign the numerical ids for volumes which would break our encoding data, so instead we need to go through and add the year data manually.
def addYear(catalog_file,jsoncatalog,output_file):
	year_mapping = {}
	with open(jsoncatalog,'r') as jsoncatalog_file:
		for entry in jsoncatalog_file:
			entry_dict = json.loads(entry)
			year_mapping[entry_dict['filename']] = entry_dict['date_year']

	with open(output_file,'w') as fixed_catalog_file:
		with open(catalog_file,'r') as broken_catalog_file:
			for line in broken_catalog_file:
				first_tab = line.find('\t')+1
				htid = line[first_tab:line.find('\t',first_tab)]
				fixed_catalog_file.write(line[:-1] + str(year_mapping[htid]) + '\n')

if __name__ == "__main__":
	addYear(sys.argv[1],sys.argv[2],sys.argv[3])