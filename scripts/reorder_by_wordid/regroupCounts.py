import argparse, csv, os, gc
import multiprocessing as mp
from functools import partial
from tqdm import tqdm
import logging

if os.name == 'nt':
	SLASH = '\\'
else:
	SLASH = '/'

def processFile(source_folder,target_folder,file_number):
	structured_memory = {}
	with open(source_folder + file_number + '.txt','r') as infile:
		source_file_reader = csv.reader(infile,delimiter='\t')
		for row in source_file_reader:
			if row[0] not in structured_memory:
				structured_memory[row[0]] = [[row[1],row[2]]]
			else:
				structured_memory[row[0]].append([row[1],row[2]])

	with open(target_folder + file_number + '.txt','w') as outfile:
		output_writer = csv.writer(outfile,delimiter='\t')
		for wordid in structured_memory:
			for count in structured_memory[wordid]:
				output_writer.writerow([wordid,count[0],count[1]])

	gc.collect()

def regroupCounts(args):
	if args.source_directory[-1:] != SLASH:
		args.source_directory = args.source_directory + SLASH

	if args.target_directory[-1:] != SLASH:
		args.target_directory = args.target_directory + SLASH

	source_files = []
	with open(args.multiwordid_file_list,'r') as source_files_list:
		source_files_reader = csv.reader(source_files_list)
		for row in source_files_reader:
			source_files.append(row[0])

	manager = mp.Manager()
	q = manager.Queue()
	cores = int(args.core_count)
	pool = mp.Pool(cores,maxtasksperchild=1000)

	process_function = partial(processFile,args.source_directory,args.target_directory)

	result_list = []

	for result in tqdm(pool.imap_unordered(process_function,source_files)):
		result_list.append(result)

if __name__ == "__main__":
	mp.set_start_method("spawn")
	parser = argparse.ArgumentParser()
	parser.add_argument("multiwordid_file_list", help="File to write results to")
	parser.add_argument("source_directory", help="Folder we're reading the files from")
	parser.add_argument("target_directory", help="Folder we're writing the output files to")
	parser.add_argument("core_count", help="Number of cores you want to devote to the process")
	args = parser.parse_args()

	regroupCounts(args)