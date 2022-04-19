import argparse, json
from tqdm import tqdm

def findFilesWithMultipleWordids(args):
	single_files_found = []
	multiwordid_files_found = []
	with open(args.file_mapping,'r') as mapping_file:
		file_mappings = json.load(mapping_file)

	with open(args.multiwordid_file_list,'a') as outfile:
		for wordid in tqdm(file_mappings):
			if len(file_mappings[wordid]) == 1:
				if file_mappings[wordid][0] not in single_files_found:
					single_files_found.append(file_mappings[wordid][0])
				else:
					if file_mappings[wordid][0] not in multiwordid_files_found:
						multiwordid_files_found.append(file_mappings[wordid][0])
						outfile.write(str(file_mappings[wordid][0]) + '\n')

if __name__ == "__main__":
	parser = argparse.ArgumentParser()
	parser.add_argument("file_mapping", help="File that contains the mapping from wordid to filename")
	parser.add_argument("multiwordid_file_list", help="File to write results to")
	args = parser.parse_args()

	findFilesWithMultipleWordids(args)