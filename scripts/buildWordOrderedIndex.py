import sys, os, json, argparse, gc, glob, csv
import multiprocessing as mp
from functools import partial
import logging

if os.name == 'nt':
	SLASH = '\\'
else:
	SLASH = '/'

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
					print("Recorded that wordid %s has been processed" % results)
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

	successfile = "successful-ordering.txt"

	manager = mp.Manager()
	q = manager.Queue()
	pool = mp.Pool(int(args.core_count))

	watcher = pool.apply_async(listener, (q,successfile))
#	jobs = []

#	for wordid in range(0,8271555):
#		job = pool.apply_async(buildIndexOf,(str(wordid),args.source_directory,args.target_directory,args.file_size,'ordered_index/',q))
#		jobs.append(job)
#
#	for job in jobs:
#		job.get()

	iterable = range(0,8271555)
	func = partial(buildIndexOf,args.source_directory,args.target_directory,args.file_size,'ordered_index/',q)
	pool.imap_unordered(func,iterable)

	pool.close()
	pool.join()
	q.put('kill')

if __name__ == "__main__":
	mp.set_start_method("spawn")
	parser = argparse.ArgumentParser()
	parser.add_argument("source_directory", help="Folder we're reading the files from")
	parser.add_argument("target_directory", help="Folder we're writing the output files to")
	parser.add_argument("core_count", help="Number of cores you want to devote to the process")
	parser.add_argument("file_size", help="Approximage max size of output files in MB")
	args = parser.parse_args()

	buildWordOrderedIndex(args)