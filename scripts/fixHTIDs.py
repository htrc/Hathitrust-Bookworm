import sys, json

def fixHTIDs(input_file,output_file):
	with open(output_file,'w') as out_write:
		with open(input_file,'r') as readfile:
			for line in readfile:
				volume = json.loads(line)
				libid, volid = volume['filename'].split('.', 1)
				volid_clean = volid.replace("+", ":").replace("=", "/").replace(",", ".")
				volume['filename'] = '.'.join([libid, volid_clean])
				out_write.write(json.dumps(volume) + '\n')

#	for volume in jsoncatalog:
#		libid, volid = volume['filename'].split('.', 1)
#		volid_clean = volid.replace("+", ":").replace("=", "/").replace(",", ".")
#		volume['filename'] = '.'.join([libid, volid_clean])
#
#	with open(output_file,'w') as out_write:
#		json.dump(jsoncatalog,out_write,indent=4)

if __name__ == "__main__":
	fixHTIDs(sys.argv[1],sys.argv[2])