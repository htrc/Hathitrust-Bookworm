import sys, os, json

if os.name == 'nt':
	SLASH = '\\'
else:
	SLASH = '/'

#Take list of ids text file that is used for building wordcounts and convert it to JSON where 
#the HTID is the key and the value is the numreical id we encode it as
def encodeVolumes(vol_ids,output_folder):
	if output_folder[-1:] != SLASH:
		output_folder = output_folder + SLASH

	vol_encodings = {}
	with open(vol_ids,'r') as vol_ids_text:
		line = vol_ids_text.readline()
		counter = 1
		while line:
			vol_encodings[line.strip()] = counter
			line = vol_ids_text.readline()
			counter = counter + 1

	with open(output_folder + 'volumelist.json','w') as outfile:
		json.dump(vol_encodings,outfile)

if __name__ == "__main__":
	encodeVolumes(sys.argv[1],sys.argv[2])