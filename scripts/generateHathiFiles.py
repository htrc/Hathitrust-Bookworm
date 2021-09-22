import sys, csv
from tqdm import tqdm
csv.field_size_limit(sys.maxsize)

def __id_encode(s: str) -> str:
	return s.replace('/', '=').replace(':', '+').replace('.', ',')

def volid_to_stubby(volid: str, ef_ext: str = '.json.bz2') -> str:
	assert '.' in volid
	lib_id, unclean_volid = volid.split('.', 1)
	clean_volid = __id_encode(unclean_volid)
	stubby_path = os.path.join(lib_id, clean_volid[::3])
	return os.path.join(stubby_path, "{}.{}{}".format(lib_id, clean_volid, ef_ext))

def generateHathiFiles(full_hathifile,volumes,output):
#	print(full_hathifile)

	volume_list = []
	volume_dict = {}
	with open(volumes,'r') as volumes_infile:
		reader = csv.reader(volumes_infile)
		for row in reader:
			lib_id, unclean_volid = row[0].split('.', 1)
			stubbied_volid = unclean_volid[::3]
			if lib_id not in volume_dict:
				volume_dict[lib_id] = {}
				volume_dict[lib_id][stubbied_volid] = [ row[0] ]
			else:
				if stubbied_volid in volume_dict[lib_id]:
					volume_dict[lib_id][stubbied_volid].append(row[0])
				else:
					volume_dict[lib_id][stubbied_volid] = [ row[0] ]

			volume_list.append(row[0])

#	print(volume_list)
	print(len(volume_list))
	found = []

	with open(output,'a') as outfile:
		hathi_writer = csv.writer(outfile,delimiter='\t')
		with open(full_hathifile,'r') as hathi_infile:
			hathi_reader = csv.reader(hathi_infile,delimiter='\t')
			line_number = 0
			for row in hathi_reader:
				print('Processing line number %d' % (line_number), end='\r')

				lib_id, unclean_volid = row[0].split('.', 1)
				stubbied_volid = unclean_volid[::3]
				if lib_id in volume_dict and stubbied_volid in volume_dict[lib_id] and row[0] in volume_dict[lib_id][stubbied_volid]:
#				if row[0] in volume_list:
					hathi_writer.writerow(row)
					found.append(row[0])

				line_number = line_number + 1

	found_set = set(found)
	with open('htids_hathifile_set.csv','w') as reduced_htids:
		for htid in found_set:
			reduced_htids.write(htid + '\n')

	print("Number of htids not found: %d" % (len(set(volume_list) - found_set)))
	print("Number of htids found: %d" % (len(found_set)))

if __name__ == "__main__":
	generateHathiFiles(sys.argv[1],sys.argv[2],sys.argv[3])