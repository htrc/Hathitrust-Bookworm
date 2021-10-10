from htrc_features import Volume, utils
import pandas as pd
from tqdm import tqdm # Progress bars!
import multiprocessing as mp
import numpy as np
import logging
import sys, os, gc

def get_last(storefile):
	with pd.HDFStore(storefile, mode="a") as store:
		n = int(store.get_storer("/tf/docs").nrows)
		return store.select_column('/tf/docs', 'id', start=n-1)

def check_for_processed(storefile,features,q):
	all_unique = []
	batchsize = 100000000

	print(os.getpid())
	with pd.HDFStore(storefile, mode="r") as store:
		# Rejecting files where the last row was not mdp
		try:
			n = int(store.get_storer("/tf/docs").nrows)
		except:
			logging.exception("Can't get row count for %s, moving on" % storefile)
			q.put([])
			return []
		try:
			a = store.select_column('/tf/docs', 'id', start=n-2)
			if a.str.split(".")[0][0] != 'mdp':
				logging.info("%s didn't process mdp most recently, skipping." % storefile)
				q.put([])
				return []
		except:
			logging.exception("Error with %s" % storefile)
			q.put([])
			return []

		logging.info("Figuring out what is already processed.")
		already_processed = get_processed()

		logging.info("Going through file backwards until all the volume ids are in the success list")

		while True:
			try:
				logging.info("Processing %s from %d" % (storefile, n-batchsize))
				startrow = (n - batchsize) if n > batchsize else 0
				unique = store.select_column('/tf/docs', 'id', start=startrow, stop=n).unique()
				uniquemdp = unique[np.char.startswith(unique.astype(np.unicode), "mdp")]
				as_paths =  pd.Series(uniquemdp).apply(lambda x: features + utils.id_to_rsync(x)).values

				to_process = np.setdiff1d(as_paths, already_processed)
				if to_process.shape[0] == 0:
					logging.info("Done at %d" % (n-batchsize))
					break
				else:
					n -= batchsize
					all_unique.append(to_process)
			except:
				n -= batchsize
				logging.exception("Error with %s from %d)" % (storefile, n))
			try:
				gc.collect()
			except:
				logging.exception("gc error")
	if len(all_unique) > 0:
		try:
			q.put(np.unique(np.concatenate(all_unique)))
			return np.unique(np.concatenate(all_unique))
		except:
			logging.exception("problem with array concatenatation, returning list")
			q.put(all_unique)
			return all_unique
	else:
		q.put([])
		return []

def get_doc_counts(paths, data, q, mincount=False, max_str_bytes = 50):
	'''
	This method lets you process multiple paths at a time on a single engine.
	This means the engine can collect enough texts to do a simple filter (i.e. >X counts in Y texts)
	and can save to it's own store.
	'''
	fname = data + 'stores/bw_counts_%s.h5' % os.getpid()
	success_log = []
	logging.info("Starting %d volume batch on PID=%s" % (len(paths), os.getpid()))
	with pd.HDFStore(fname, mode="a", complevel=9, complib='blosc') as store:
		tl_collector = []
		for path in paths:
			logging.info("Trying %s" % path)
			logging.info("%f MB" % (os.stat(path).st_size / (1024.0 * 1024)))
			try:
				tl = get_count(path, store=store)
				if tl.empty:
					logging.info("%s is empty" % path)
					continue
				tl_collector.append(tl)
			except:
				logging.exception("Unable to get count for path %s" % path)
				continue
			success_log.append(path)

		# Save a DF combining all the counts from this batch
		try:
			logging.info("Merging and Saving texts for %d paths starting with %s" % (len(paths), paths[0]))
			combineddf = pd.concat(tl_collector)
			logging.info("Created combined df")

			# Save tf(doc) with volid but no lang
			# For efficient HDF5 storage, enforcing a 50 byte token limit. Can't use
			# DataFrame.str.slice(stop=50) though, because really we care about bytes and 
			# some unicode chars are multiple codepoints.
			# volids are capped at 25chars (the longest PD vol id)
			combineddf_by_volid = combineddf.reset_index('language')[['count']]
			logging.info("Reset index")
			logging.info("Number of rows: %d" % len(combineddf.index))
			logging.info("Size of DataFrame: %f MB" % (combineddf.memory_usage(deep=True).sum() / (1024 * 1024)))
		#	logging.info("%s" % combineddf.info(memory_usage='deep'))
			store.append('/tf/docs',
						combineddf_by_volid,
						min_itemsize = {'id': 25, 'token':max_str_bytes})
			logging.info("Appended volid and token")

			### Save tf(corpus)
			df = combineddf.groupby(level=['language', 'token'])[['count']]\
							.sum().sort_index()
			logging.info("Grouped by language")
			# Filtering this way (by corpus total, not language total) is too slow:
			#if mincount:
			#    df = df.groupby(level='token')[['count']].filter(lambda x: x.sum()>=mincount)
			# Because we can't feasibly filter on total count and have to do so by lang x token, it
			# might unfairly punish sparse languages. My workaround is to only even trim English by
			# mincount: any bias this would have would be in the bottom of the wordlist anyway.
			if mincount:
				df = df[(df.index.get_level_values(0) != 'eng') | (df['count']>2)]
			store.append('tf/corpus', df, min_itemsize = {'token': max_str_bytes})
			logging.info("Appended language and token")
			tl_collector = dict()
			q.put(success_log)
			return success_log
		except:
			logging.exception("Saving error for %d paths starting with %s" % (len(paths), paths[0]))
			q.put([])
			return []
	gc.collect()

	q.put(paths)
	return paths

def get_count(path, store=False):
	''' Get tokencount information from a single doc, by path''' 
	max_char = 50
	logging.debug(path)
	vol = Volume(path)
	logging.debug(vol)
	logging.debug(vol.id)
	logging.debug(vol.language)
	logging.debug(type(vol.language))
	tl = vol.tokenlist(pages=False, pos=False)
	if tl.empty:
		return tl
	else:
		tl = tl.reset_index('section')[['count']]
	tl.index = [trim_token(t, max_char) for t in tl.index.values]
	tl.index.names=['token']
	tl['id'] = vol.id
	if type(vol.language) is list:
		tl['language'] = vol.language[0]
	else:
		tl['language'] = vol.language
	tl = tl.reset_index('token').set_index(['language', 'id', 'token']).sort_index()
	logging.debug(tl)
	return tl

def trim_token(t, max=50):
	''' Trim unicode string to max number of bytes'''
	if len(t.encode('utf-8')) > max:
		while len(t.encode('utf-8')) > max:
			t = t[:-1]
	return t

def get_processed(features):
	''' Get already processed files. Wrapped in func for easy refresh'''
	try:
		with open(successfile, "r") as f:
			paths = f.read().strip().split("\n")
		paths = [features+utils.id_to_rsync(path) for path in paths]
		return np.array(paths)
	except:
		return np.array([])

def init_log(data,name=False):
	if not name:
		name = os.getpid()
	handler = logging.FileHandler(data + "logs/bw-%s.log" % name, 'a')
	formatter = logging.Formatter('%(asctime)s:%(levelname)s:%(message)s', "%m/%d-%H:%M:%S")
	handler.setFormatter(formatter)
	logger = logging.getLogger()
	logger.setLevel(logging.INFO)
	logger.addHandler(handler)
	logging.info("Log initialized")

def listener(q,successfile):
	path_to_id = lambda x: x.replace(".json.bz2", "").split("/")[-1]
	i = 0

	while(1):
		results = q.get()

		i += 1
		if results:
#			print("Got Results")
#			print(os.getpid())
			if results == 'kill':
#				print("Results say kill")
				break
			else:
#				print("Results say write")
#				print(results)
				with open(successfile, "a+") as f:
					ids =[path_to_id(path) for path in results]
#					print(ids)
					f.write("\n".join(ids)+"\n")
				logging.info("Done processing batch %d, from %s to %s" % (i, results[0], results[-1]))
		else:
			print("No Results")
			logging.error("Problem with result in batch %d" % i)

def store_check_listener(q,successfile):
	all_ids = []
	i = 0

	while(1):
		results = q.get()
		print(results)

		if results:
			if results == 'kill':
#				print("Results say kill")
				uniqueids = np.unique(np.concatenate(all_ids))
				np.save("addtosuccessful2", uniqueids)

				a = pd.Series(uniqueids)
				b = a[a.astype(str).str.find("mdp") >= 0]
				c = get_processed()
				d = np.setdiff1d(b.values, c)
				e = pd.Series(d).apply(lambda x: x.split("/")[-1].split(".json")[0]).values
				with open(successfile, "a+") as f:
					f.write("\n".join(e)+"\n")

				print(remaining_paths.shape)
				remaining_paths = np.setdiff1d(paths, get_processed())
				print(remaining_paths.shape)
				break
			else:
				i += 1
				all_ids.append(results)
				logging.info("Batch %d done" % i)


def generateStores(features,data,core_count):
	init_log(data,"root")

	with open(features+"listing/ids.txt", "r") as f:
		paths = [features+path.strip() for path in f.readlines()]
		print("Number of texts", len(paths))

	successfile = data + "successful-counts.txt"
	get_count(paths[0]).head(3)

	import time
	# Split paths into N-sized chunks, so engines can iterate on multiple texts at once
	chunk_size = 25
	remaining_paths = np.setdiff1d(paths, get_processed(features))
	print("%d paths remaining" % len(remaining_paths))
	n = 10000000
	start = 0
	chunked_paths = [remaining_paths[start+i:start+i+chunk_size] for i in range(0, len(remaining_paths[start:start+n]), chunk_size)]

	print(os.getpid())
	manager = mp.Manager()
	q = manager.Queue()
	p = mp.Pool(int(core_count),initializer=init_log,initargs=(data,),maxtasksperchild=35000)

	starttime = time.time()
	logging.info("Starting parallel job")

	watcher = p.apply_async(listener, (q,successfile))

	jobs = []
	for chunked_path in chunked_paths:
		job = p.apply_async(get_doc_counts,(chunked_path,data,q))
		jobs.append(job)

	for job in tqdm(jobs):
		job.get()


	logging.info("Done")
	logging.info(time.time()-starttime)
	q.put('kill')

	import glob
	storestocheck = glob.glob(data + "stores/*h5")
	watcher = p.apply_async(store_check_listener, (q,successfile))
	store_check_jobs = []
	last = []
	for store in storestocheck:
		print(store)
		last.append(get_last(store))
		print(last[-1])
		store_check_job = p.apply_async(check_for_processed,(store,features,q))
		store_check_jobs.append(store_check_job)
	print(last)

	for store_check_job in tqdm(store_check_jobs):
		store_check_job.get()

	q.put('kill')
	p.close()
	p.join()


if __name__ == "__main__":
	generateStores(sys.argv[1],sys.argv[2],sys.argv[3])