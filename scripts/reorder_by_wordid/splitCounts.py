import sys, os, csv, argparse, gc
import multiprocessing as mp
from functools import partial
from tqdm import tqdm

if os.name == 'nt':
	SLASH = '\\'
else:
	SLASH = '/'

def splitFile(source_directory,target_directory,source_file):
	with open(target_directory + source_file[:-4] + '_0.txt','w') as evenfile:
		evenfile_writer = csv.writer(evenfile,delimiter='\t')
		with open(target_directory + source_file[:-4] + '_1.txt','w') as oddfile:
			oddfile_writer = csv.writer(oddfile,delimiter='\t')
			with open(source_directory + source_file,'r') as infile:
				source_file_reader = csv.reader(infile,delimiter='\t')
				for row in source_file_reader:
					if int(row[0]) % 2:
						oddfile_writer.writerow(row)
					else:
						evenfile_writer.writerow(row)

def splitCounts(args):
	if args.source_directory[-1:] != SLASH:
		args.source_directory = args.source_directory + SLASH

	if args.target_directory[-1:] != SLASH:
		args.target_directory = args.target_directory + SLASH

	input_files = [f for f in os.listdir(args.source_directory) if f[-4:] == '.txt']

	manager = mp.Manager()
	q = manager.Queue()
	cores = int(args.core_count)
	pool = mp.Pool(cores,maxtasksperchild=1000)

	split_function = partial(splitFile,args.source_directory,args.target_directory)

	result_list = []

	for result in tqdm(pool.imap_unordered(split_function,input_files)):
		result_list.append(result)

if __name__ == "__main__":
	mp.set_start_method("spawn")
	parser = argparse.ArgumentParser()
	parser.add_argument("source_directory", help="Folder we're reading the files from")
	parser.add_argument("target_directory", help="Folder we're writing the output files to")
	parser.add_argument("core_count", help="Number of cores you want to devote to the process")
	args = parser.parse_args()

	splitCounts(args)