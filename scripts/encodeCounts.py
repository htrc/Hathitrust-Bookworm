import sys, os, json, argparse, gc, glob
import pandas as pd
import multiprocessing as mp
import numpy as np
from tqdm import tqdm
import logging

if os.name == 'nt':
	SLASH = '\\'
else:
	SLASH = '/'

def applyEncoding(raw_items,mapping):
	output = []
	for item in raw_items:
		try:
			output.append(mapping[item])
		except:
			output.append(item)

	return output

def listener(q):
	i = 0

	while(1):
		results = q.get()
		i += 1

		if results:
			if results == 'kill':
				break
			else:
				if results == "success":
					print("Done processing batch %d" % i)
				else:
					print(results)


def processChunk(chunk,word_dict,vol_dict,output_folder,store_name,output_file_size):
	print("%s processing a chunk of %i volumes" % (str(os.getpid()),len(chunk['count'].index.levels[0].values)))
	chunk['count'].index = chunk['count'].index.set_levels(applyEncoding(chunk['count'].index.levels[0].values,vol_dict),level=0)

	drop_list = []
	encoded_index = []
	for ind in chunk['count'].index.values:
		try:
			encoded_index.append((ind[0],word_dict[ind[1]]))
		except:
			drop_list.append(ind)

	chunk.drop(drop_list,inplace=True)
	encoded_df = pd.DataFrame(data=chunk['count'].values,index=pd.MultiIndex.from_tuples(encoded_index))

	output_base = 'tmp-count-' + str(os.getpid())
	output_files = [ x for x in os.listdir(output_folder + store_name) if output_base in x ]
	if output_files:
		biggest_value = 0
		for output_file in output_files:
			if int(output_file[output_file.rfind('-')+1:-4]) > biggest_value:
				biggest_value = int(output_file[output_file.rfind('-')+1:-4])

		if os.stat(output_folder + store_name + SLASH + output_base + '-' + str(biggest_value) + '.txt').st_size > int(output_file_size) * 1024 * 1024:
			encoded_df.to_csv(output_folder + store_name + SLASH + output_base + '-' + str(biggest_value + 1) + '.txt',mode='a',header=False,sep='\t')
		else:
			encoded_df.to_csv(output_folder + store_name + SLASH + output_base + '-' + str(biggest_value) + '.txt',mode='a',header=False,sep='\t')
	else:
		encoded_df.to_csv(output_folder + store_name + SLASH + output_base + '-0.txt',mode='a',header=False,sep='\t')

	return "success"

def parallelEncodeH5File(core_count,counts,word_dict,vol_dict,output_folder,output_file_size):
	store_iterator = pd.read_hdf(counts,key='/tf/docs',iterator=True,chunksize=1000000)
	store_name = counts[counts.rfind('/')+1:-3]
	os.mkdir(output_folder + store_name)

	manager = mp.Manager()
	q = manager.Queue()
	pool = mp.Pool(int(core_count))

	watcher = pool.apply_async(listener, (q,))

	jobs = []
	
	file_chunk_counter = 0
	for chunk in store_iterator:
		print("Adding chunk:")
		print(chunk)
		job = pool.apply_async(processChunk,(chunk,word_dict,vol_dict,output_folder,store_name,output_file_size))
		jobs.append(job)

	for job in jobs:
		print("Running job:")
		print(job)
		job.get()

	q.put('kill')
	pool.close()
	pool.join()

def init_log(data,name=False):
	if not name:
		name = os.getpid()
	handler = logging.FileHandler(data + "logs/bw-%s.log" % name, 'a')
	formatter = logging.Formatter('%(asctime)s:%(levelname)s:%(message)s', "%m/%d-%H:%M:%S")
	handler.setFormatter(formatter)
	logger = logging.getLogger()
	logger.setLevel(logging.DEBUG)
	logger.addHandler(handler)
	logging.info("Log initialized")

def get_unencoded_stores(rawstores,successfile,store_path):
	try:
		with open(successfile, "r") as f:
			paths = f.read().strip().split("\n")
		return np.setdiff1d([ r[len(store_path + 'bw_counts_'):-3] for r in rawstores ],paths,True)
	except:
		return [ r[len(store_path + 'bw_counts_'):-3] for r in rawstores ]

def remove_incomplete_encodings(unencoded_store_ids,encoded_count_files,output_folder):
	for f in encoded_count_files:
		if f[len(output_folder + 'tmp-count-'):f.rfind('-')] in unencoded_store_ids:
			os.remove(f)

def encodeH5File(counts,word_dict,vol_dict,output_folder,output_file_size,q):
	try:
		store_iterator = pd.read_hdf(counts,key='/tf/docs',iterator=True,chunksize=100000)
		store_name = counts[counts.rfind('/')+1:-3]
		store_number = store_name[store_name.rfind("_")+1:]
	#	os.mkdir(output_folder + store_name)

		file_chunk_counter = 0
		print("%s: Processing %s" % (str(os.getpid()),store_name))
		for chunk in store_iterator:
			logging.info("%s – Processing a chunk of %i volumes from %s" % (str(os.getpid()),len(chunk['count'].index.levels[0].values),store_name))
			logging.debug("Processing the following volumes:")
			logging.debug(chunk['count'].index.levels[0].values)
			chunk['count'].index = chunk['count'].index.set_levels(applyEncoding(chunk['count'].index.levels[0].values,vol_dict),level=0)

			drop_list = []
			encoded_index = []
			for ind in chunk['count'].index.values:
				try:
					encoded_index.append((ind[0],word_dict[ind[1]]))
				except Exception as e:
					drop_list.append(ind)

			logging.debug(drop_list[0])
			logging.debug(chunk['count']['6952'])
			logging.debug(type(chunk['count']['6952']))
			chunk.drop(drop_list,inplace=True)
			logging.debug("Finished dropping")

			if len(encoded_index) > 0:
				encoded_df = pd.DataFrame(data=chunk['count'].values,index=pd.MultiIndex.from_tuples(encoded_index))

				encoded_df.to_csv(output_folder + 'tmp-count-' + store_number + '-' + str(file_chunk_counter) + '.txt',mode='a',header=False,sep='\t')
				if os.stat(output_folder + 'tmp-count-' + store_number + '-' + str(file_chunk_counter) + '.txt').st_size > int(output_file_size) * 1024 * 1024:
					file_chunk_counter = file_chunk_counter + 1

			gc.collect()

		logging.info("Finished processing %s, sending store number to successfile" % counts)
		q.put(store_number)
	except Exception as e:
		logging.debug("Error while processing store %s" % counts)
		logging.error(e)

	gc.collect()

def listener(q,successfile):
	while(1):
		results = q.get()

		if results:
			if results == 'kill':
				break
			else:
				try:
					with open(successfile,'a') as f:
						f.write(results + "\n")
					logging.info("Recorded that store %s has been processed" % results)
				except Exception as e:
					logging.debug("Error while writing to successfile")
					logging.debug(e)

def encodeCounts(args):
	if args.output_folder[-1:] != SLASH:
		args.output_folder = args.output_folder + SLASH

	if args.counts_folder[-1:] != SLASH:
		args.counts_folder = args.counts_folder + SLASH

	init_log('encode_words/','encoding')

	successfile = "successful-encodings.txt"
	rawstores = glob.glob(args.counts_folder + "*h5")
	unencoded_store_ids = get_unencoded_stores(rawstores,successfile,args.counts_folder)
	remove_incomplete_encodings(unencoded_store_ids,glob.glob(args.output_folder + "*.txt"),args.output_folder)
	logging.info(unencoded_store_ids)
	unencoded_store_files = [ args.counts_folder + 'bw_counts_' + store_id + '.h5' for store_id in unencoded_store_ids ]
	logging.info(unencoded_store_files)

	manager = mp.Manager()
	q = manager.Queue()

	with open(args.wordlist,'r') as wordlist_file:
		word_dict = json.load(wordlist_file)

	with open(args.volumelist,'r') as volumelist_file:
		vol_dict = json.load(volumelist_file)

	if args.single_file_processing:
		print("Each .h5 file will be processed by %s parallel processes simultaniously in full before moving on to the next file. Outputs won't greatly exceed a set max size." % args.core_count)
		for file in tqdm(os.listdir(args.counts_folder)):
			if file.endswith('.h5'):
				print("Begining encoding of %s" % file)
				parallelEncodeH5File(args.core_count,os.path.join(args.counts_folder,file),word_dict,vol_dict,args.output_folder,args.file_size)
	else:
		print("Each .h5 file will be turned into an number of .txt files, each of which are around %sMB." % args.file_size)
		pool = mp.Pool(int(args.core_count),initializer=init_log,initargs=('encode_words/',),maxtasksperchild=1)

		watcher = pool.apply_async(listener, (q,successfile))
		jobs = []

		for file in unencoded_store_files:
			job = pool.apply_async(encodeH5File,(file,word_dict,vol_dict,args.output_folder,args.file_size,q))
			jobs.append(job)

		for job in jobs:
			job.get()

		q.put('kill')
		pool.close()
		pool.join()

if __name__ == "__main__":
	mp.set_start_method("spawn")
	parser = argparse.ArgumentParser()
	parser.add_argument("wordlist", help="The wordlist.json file that encodes words")
	parser.add_argument("volumelist", help="The volumelist.json file that encodes volumes")
	parser.add_argument("counts_folder", help="The folder that holds the .h5 files to count")
	parser.add_argument("output_folder", help="Foler to write the output to. Must exists")
	parser.add_argument("core_count", help="Number of cores you want to devote to the process")
	parser.add_argument("file_size", help="Approximage max size of output files in MB")
	parser.add_argument("--single_file_processing", action="store_true", help="If this flag is turned on each .h5 file will be processed by a separate process. Since that will open many files at once, this may cause memory issues.")
	args = parser.parse_args()
	
	encodeCounts(args)

#	if len(sys.argv) > 6:
#		encodeCounts(sys.argv[1],sys.argv[2],sys.argv[3],sys.argv[4],sys.argv[5],sys.argv[6])
#	else:
#		encodeCounts(sys.argv[1],sys.argv[2],sys.argv[3],sys.argv[4],sys.argv[5])