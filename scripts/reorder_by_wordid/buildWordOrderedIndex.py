import sys, os, json, argparse, gc, glob, csv
import multiprocessing as mp
from functools import partial
from tqdm import tqdm
import logging
#from memory_profiler import profile

if os.name == 'nt':
	SLASH = '\\'
else:
	SLASH = '/'

def writeWordCountsToFile(target_directory,word_counts):
	output_file = target_directory + str(word_counts['filename']) + ".txt"
	with open(output_file,'a') as open_output_file:
		output_writer = csv.writer(open_output_file,delimiter='\t')
		for entry in word_counts:
			if entry != 'filename':
#				if entry == '58':
#					print(entry)
#					print(len(word_counts[entry]))
				for row in word_counts[entry]:
					output_writer.writerow([entry,row[0],row[1]])

	gc.collect()

def readThroughFile(target_directory,file_mappings,source_directory,source_file):
	processing_memory = {}
#	print("Reading file %s" % source_directory + source_file)
	with open(source_directory + source_file) as checkfile:
		file_reader = csv.reader(checkfile,delimiter='\t')
		try:
			for row in file_reader:
				write_files = file_mappings[row[1]]
				if len(write_files) > 1:
					write_file = None
	#					print(write_files)
					for candidate_file in range(0,len(write_files)):
	#						print(str(write_files[candidate_file]) + ".txt")
#						if str(write_files[candidate_file]) + ".txt" not in worid_files:
						if not os.path.isfile(target_directory + str(write_files[candidate_file]) + ".txt"):
							write_file = write_files[candidate_file]
							break

						if os.path.getsize(target_directory + str(write_files[candidate_file]) + ".txt")/(1024*1024) < 100:
							write_file = write_files[candidate_file]
							break

					if write_file is None:
						write_file = write_files[len(write_files)-1]
				else:
					write_file = write_files[0]

				int_wordid = int(row[1])
				if write_file in processing_memory:
					if int_wordid in processing_memory[write_file]:
						processing_memory[write_file][int_wordid].append([int(row[0]),int(row[2])])
#						if str(write_file) == '113':
#							print("Case A: ")
#							print(row)
#							print(processing_memory[write_file])
					else:
						processing_memory[write_file][int_wordid] = [[int(row[0]),int(row[2])]]
#						if str(write_file) == '113':
#							print("Case B: ")
#							print(row)
#							print(processing_memory[write_file])
				else:
					processing_memory[write_file] = { 'filename': write_file, int_wordid: [[int(row[0]),int(row[2])]] }
#					if str(write_file) == '113':
#						print("Case C: ")
#						print(row)
#						print(processing_memory[write_file])
		except:
			print("Error processing file %s" % source_file)
			raise

	return processing_memory

def mergeListDicts(dicts):
	merged_dicts = {}
	for d in dicts:
		for key in d:
			if key not in merged_dicts:
				merged_dicts[key] = d[key]
			else:
				for k in d[key]:
					if k not in merged_dicts[key]:
						merged_dicts[key][k] = d[key][k]
					else:
						if k != 'filename':
							merged_dicts[key][k] += d[key][k]

	return merged_dicts 

def init_log(data,name=False):
	if not name:
		name = os.getpid()
	handler = logging.FileHandler(data + "logs/bw-%s.log" % name, 'a')
	formatter = logging.Formatter('%(asctime)s:%(levelname)s:%(message)s', "%m/%d-%H:%M:%S")
	handler.setFormatter(formatter)
	logger = logging.getLogger(str(os.getpid()))
	logger.setLevel(logging.INFO)
	logger.addHandler(handler)
	logger.info("Log initialized")
	return logger

#@profile
def buildWordOrderedIndex(args):
	if args.source_directory[-1:] != SLASH:
		args.source_directory = args.source_directory + SLASH

	if args.target_directory[-1:] != SLASH:
		args.target_directory = args.target_directory + SLASH

	if args.logging_directory[-1:] != SLASH:
		args.logging_directory = args.logging_directory + SLASH

	logger = init_log(args.logging_directory,'reorder')

	with open(args.file_mapping,'r') as mapping_file:
		file_mappings = json.load(mapping_file)

	manager = mp.Manager()
	q = manager.Queue()
	cores = int(args.core_count)
	pool = mp.Pool(cores,maxtasksperchild=int((32168/int(args.core_count))+1))

	write_func = partial(writeWordCountsToFile,args.target_directory)

	bookid_files = [f for f in os.listdir(args.source_directory)]
	processing_memory = {}
	write_result_list = []
	result_list = []

	for file_counter in range(0,len(bookid_files),int(args.core_count)):
		logger.info("Beginning to process %s" % ", ".join(bookid_files[file_counter:file_counter+int(args.core_count)]))

#		worid_files = [f for f in os.listdir(args.target_directory)]
		read_func = partial(readThroughFile,args.target_directory,file_mappings,args.source_directory)

		for result in tqdm(pool.imap_unordered(read_func,bookid_files[file_counter:file_counter+int(args.core_count)])):	
			result_list.append(result)

#		for res in result_list:
#			for file in res:
#				if res[file]['filename'] == 113:
#					print(res[file])
#					if 58 in res[file]:
#						print(len(res[file][58]))
		
		processing_memory = mergeListDicts(result_list)
#		with open('sample_memory_object.json','w') as test_file:
#			json.dump(processing_memory,test_file)
#		sys.exit()

		result_list = []
		gc.collect()

		for write_result in tqdm(pool.imap_unordered(write_func,processing_memory.values())):
			write_result_list.append(write_result)

		processing_memory = {}
		write_result_list = []
		gc.collect()

	pool.close()
	pool.join()

if __name__ == "__main__":
	mp.set_start_method("spawn")
	parser = argparse.ArgumentParser()
	parser.add_argument("source_directory", help="Folder we're reading the files from")
	parser.add_argument("target_directory", help="Folder we're writing the output files to")
	parser.add_argument("logging_directory", help="Folder where you want to write the logs to")
	parser.add_argument("file_mapping", help="JSON file that maps wordid to file where some or all of the counts will be stored")
	parser.add_argument("core_count", help="Number of cores you want to devote to the process")
	args = parser.parse_args()

	buildWordOrderedIndex(args)