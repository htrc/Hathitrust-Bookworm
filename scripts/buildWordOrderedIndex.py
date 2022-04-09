import sys, os, json, argparse, gc, glob, csv
import multiprocessing as mp
from functools import partial
import logging

if os.name == 'nt':
	SLASH = '\\'
else:
	SLASH = '/'

def writeWordCountsToFile(target_directory,word_counts):
#	print(word_counts)
	output_file = target_directory + word_counts['filename'] + ".txt"
	with open(output_file,'a') as open_output_file:
		output_writer = csv.writer(open_output_file,delimiter='\t')
		for entry in word_counts:
			if entry != 'filename':
				for row in word_counts[entry]:
					output_writer.writerow([entry,row[0],row[1]])

	gc.collect()

def writeRowToFile(output,target_directory):
#	handler = logging.FileHandler(data + "logs/bw-%i.log" % os.getpid(), 'a')
#	formatter = logging.Formatter('%(asctime)s:%(levelname)s:%(message)s', "%m/%d-%H:%M:%S")
#	handler.setFormatter(formatter)
#	logger = logging.getLogger(str(os.getpid()))
#	logger.setLevel(logging.DEBUG)
#	if logger.hasHandlers():
#		logger.handlers.clear()
#	logger.addHandler(handler)

	output_file = target_directory + str(output[0]) + ".txt"
	with open(output_file,'a') as open_output_file:
		output_writer = csv.writer(open_output_file,delimiter='\t')
		output_writer.writerow(output)

def buildIndexOf(source_directory,target_directory,output_file_size,data,q,wordid):
	handler = logging.FileHandler(data + "logs/bw-%i.log" % os.getpid(), 'a')
	formatter = logging.Formatter('%(asctime)s:%(levelname)s:%(message)s', "%m/%d-%H:%M:%S")
	handler.setFormatter(formatter)
	logger = logging.getLogger(str(os.getpid()))
	logger.setLevel(logging.DEBUG)
	if logger.hasHandlers():
		logger.handlers.clear()
	logger.addHandler(handler)
	wordid = str(wordid)

	bookid_files = [f for f in os.listdir(source_directory)]

	output_base = 'tmp-count-' + str(os.getpid())
	output_files = [ x for x in os.listdir(target_directory) if output_base in x ]
	biggest_value = 0
	if output_files:
		for output_file in output_files:
			if int(output_file[output_file.rfind('-')+1:-4]) > biggest_value:
				biggest_value = int(output_file[output_file.rfind('-')+1:-4])

		if os.stat(target_directory + output_base + '-' + str(biggest_value) + '.txt').st_size > int(output_file_size) * 1024 * 1024:
			output_file_path = target_directory + output_base + '-' + str(biggest_value + 1) + '.txt'
		else:
			output_file_path = target_directory + output_base + '-' + str(biggest_value) + '.txt'
	else:
		output_file_path = target_directory + output_base + '-0.txt'

	with open(output_file_path,'a') as outfile:
		output_writer = csv.writer(outfile,delimiter='\t')
		for file in bookid_files:
			logger.info("%i: Looking for %s in %s" % (os.getpid(),wordid,file))
			with open(source_directory + file) as checkfile:
				file_reader = csv.reader(checkfile,delimiter='\t')
				for row in file_reader:
					if row[1] == wordid:
						output_writer.writerow([row[1],row[0],row[2]])

			gc.collect()

	q.put(wordid)
	gc.collect()

def listener(q,successfile):
	while(1):
		results = q.get()

		if results:
			print(results)
			if results == 'kill':
				break
			else:
				try:
					with open(successfile,'a') as f:
						f.write(results + "\n")
					print("Recorded that file %s has been processed" % results)
				except Exception as e:
					print("Error while writing to successfile")
					print(e)

def init_log(data,name=False):
	if not name:
		name = os.getpid()
	handler = logging.FileHandler(data + "logs/bw-%s.log" % name, 'a')
	formatter = logging.Formatter('%(asctime)s:%(levelname)s:%(message)s', "%m/%d-%H:%M:%S")
	handler.setFormatter(formatter)
	logger = logging.getLogger(str(os.getpid()))
	logger.setLevel(logging.DEBUG)
	logger.addHandler(handler)
	logging.info("Log initialized")

def buildWordOrderedIndex(args):
	if args.source_directory[-1:] != SLASH:
		args.source_directory = args.source_directory + SLASH

	if args.target_directory[-1:] != SLASH:
		args.target_directory = args.target_directory + SLASH

	init_log('ordered_index/','encoding')

#	successfile = "successfuly-processed.txt"

	manager = mp.Manager()
	q = manager.Queue()
	procs = manager.dict()
	cores = int(args.core_count)
	pool = mp.Pool(cores)

#	watcher = pool.apply_async(listener, (q,successfile))
	processing = []

#	for wordid in range(0,8271555):
#		job = pool.apply_async(buildIndexOf,(str(wordid),args.source_directory,args.target_directory,args.file_size,'ordered_index/',q))
#		jobs.append(job)
#
#	for job in jobs:
#		job.get()

#	iterable = range(0,8271555)
#	func = partial(buildIndexOf,args.source_directory,args.target_directory,args.file_size,'ordered_index/',q)
#	pool.imap_unordered(func,iterable)

	func = partial(writeWordCountsToFile,args.target_directory)

	with open(args.file_mapping,'r') as mapping_file:
		file_mappings = json.load(mapping_file)

	bookid_files = [f for f in os.listdir(args.source_directory)]
	for file in bookid_files:
		print("Beginning to process %s" % file)
		processing_memory = {}
		worid_files = [f for f in os.listdir(args.target_directory)]
		with open(args.source_directory + file) as checkfile:
			file_reader = csv.reader(checkfile,delimiter='\t')
			for row in file_reader:
				write_files = file_mappings[row[1]]
				if len(write_files) > 1:
					write_file = None
#					print(write_files)
					for candidate_file in range(0,len(write_files)):
#						print(str(write_files[candidate_file]) + ".txt")
						if str(write_files[candidate_file]) + ".txt" not in worid_files:
							write_file = write_files[candidate_file]
							break

						if os.path.getsize(args.target_directory + str(write_files[candidate_file]) + ".txt")/(1024*1024) < 100:
							write_file = write_files[candidate_file]
							break

					if write_file is None:
						write_file = write_files[len(write_files)-1]
				else:
					write_file = write_files[0]

				if write_file in processing_memory:
					if row[1] in processing_memory[write_file]:
						processing_memory[write_file][row[1]].append([row[0],row[2]])
					else:
						processing_memory[write_file] = { 'filename': str(write_file), row[1]: [[row[0],row[2]]] }
				else:
					processing_memory[write_file] = { 'filename': str(write_file), row[1]: [[row[0],row[2]]] }
#				if row[1] in processing_memory:
#					processing_memory[row[1]].append([row[1],row[0],row[2]])
#				else:
#					processing_memory[row[1]] = [[row[1],row[0],row[2]]]
#			print(processing_memory)
#			sys.exit()
		gc.collect()
		print("Starting parallel writes on %s" % file)
		pool.imap_unordered(func,processing_memory.values())
#		print(processing_memory)
#		sys.exit()
#					print("Waiting on %s" % row[1])
#					print(processing)
#					time.sleep(2)
#				pool.apply_async(writeRowToFile,([row[1],row[0],row[2]],args.target_directory))
#				processing.append(row[1])
#				if len(processing) == cores:
#					processing.pop(0)
#				output_writer.writerow([row[1],row[0],row[2]])
#		q.put(file)

	pool.close()
	pool.join()
#	q.put('kill')

if __name__ == "__main__":
	mp.set_start_method("spawn")
	parser = argparse.ArgumentParser()
	parser.add_argument("source_directory", help="Folder we're reading the files from")
	parser.add_argument("target_directory", help="Folder we're writing the output files to")
	parser.add_argument("file_mapping", help="JSON file that maps wordid to file where some or all of the counts will be stored")
	parser.add_argument("core_count", help="Number of cores you want to devote to the process")
#	parser.add_argument("file_size", help="Approximage max size of output files in MB")
	args = parser.parse_args()

	buildWordOrderedIndex(args)