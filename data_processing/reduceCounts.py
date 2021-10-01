import glob
import re
import pandas as pd
import numpy as np
import multiprocessing as mp
import logging
import os
import gc
from tqdm import tqdm # Progress bars!
import dask.dataframe as dd
from dask.diagnostics import ProgressBar, Profiler, ResourceProfiler, CacheProfiler, visualize

def finalSort(data):
	with pd.HDFStore(data + 'final/final.h5') as store:
		keys = store.keys()

	with ProgressBar(), Profiler() as prof:
		with pd.HDFStore(data + 'final/final-sorted.h5', complevel=9, complib='blosc') as outstore:
			for key in keys:
				logging.info("Sorting %s" % key)
				df = dd.read_hdf(data + 'final/final.h5', key)
				sortdf = df.compute().sort_values('count', ascending=False)
				outstore.append(key, sortdf)
		logging.info("Done sorting")

def finalCombine(stores,data):
	# Collect a list of which stores have information for each possible language
	storelist = dict()
	for storepath in stores:
		with pd.HDFStore(storepath) as store:
			for key in store.keys():
				print(key)
				print(storepath)
				if key in storelist:
					storelist[key].append(storepath)
				else:
					storelist[key] = [storepath]

	query = "count >= 10"
	listfilter = '/[a-z]'
	processlist = [item for item in storelist.items() if re.match(listfilter, item[0])]
	print(processlist)

	logging.info("Processing %s, filtered to %s" % (", ".join([p[0] for p in processlist]), query))

	for lang, langstores in processlist:
		try:
			# Get dask dataframe for given language from multiple sources
			dask_dfs = [dd.read_hdf(path, lang, chunksize=100000) for path in langstores]
			ddf = dd.concat(dask_dfs)
			logging.info("Processing %s with %d partitions" % (lang, ddf.npartitions))

			with ProgressBar():
				ddf.query(query).reset_index().groupby('token').sum().to_hdf(data + 'final/final.h5', lang, complevel=9, complib='blosc')
		except:
			logging.exception("Error with %s" % lang)
	logging.info("Done")


def sumTokenCounts(stores,data):
	max_str_bytes = 50
	chunksize = 100000
	batch_limit = 6*10**8
	savestore = data + "final/fromnodes-323.h5"

	for storefile in stores:
		print(storefile)
		logging.info("Next store: %s" % storefile)
		try:
			# Get Unique languages
			with pd.HDFStore(storefile, complevel=9, mode="a", complib='blosc') as store:
				langs = set([key.split("/", maxsplit=-1)[-1] for key in store.keys() if 'merged1' in key])
		except:
			logging.exception("Can't read languages from %s" % storefile)
			continue

		for lang in langs:
			batch = False
			logging.info("Starting lang %s from %s" % (lang, storefile))
			print(lang)

			if not re.match('[a-z]{3}', lang):
				logging.error("lang '%s' is not three alphanumeric characters. Skipping for now. (%s)" % (lang, storefile))
				continue

			try:
				ddf = dd.read_hdf(storefile, '/merged1/'+lang, chunksize=chunksize, mode='r')
			except:
				logging.exception("Can't load Dask DF for %s in %s" % (lang, storefile))
				continue

			# Assuming partitions are equally sized, which they should be if read from a single file
			if ddf.npartitions > np.ceil(batch_limit/chunksize):
				batch = True
				niters = np.floor((ddf.npartitions*chunksize)/batch_limit)
				i = 0

			while True:
				if batch:
					start = i * batch_limit
					logging.info("Starting batch %d for %s" % (i, lang))
					if i == niters:
						# Last batch, no stop value
						ddf = dd.read_hdf(storefile, '/merged1/'+lang, chunksize=chunksize, start=start)
						batch = False
					else:
						ddf = dd.read_hdf(storefile, '/merged1/'+lang, chunksize=chunksize,start=start, stop=(start+batch_limit))
						i += 1
				try:
					logging.info("Starting full merge for %s with %d partitions" % (lang, ddf.npartitions))
					with ProgressBar():
						full_merge = ddf.reset_index().groupby('token').sum().compute()
					#if lang == 'eng':
						# For curiosity: see the profiling for English
					#    prof.visualize()
					logging.info("Success! Saving merged.")
					# The /fromnodes table is the sum from all the different stores, but will need to be summed one more time
					with pd.HDFStore(savestore, complevel=9, mode="a", complib='blosc') as store:
						store.append(lang,full_merge,data_columns=['count'],min_itemsize = {'index': max_str_bytes})
				except:
					logging.exception("Can't compute or save lang for %s in %s" % (lang, storefile))

				if batch == False:
					break

		logging.info("Finished processing %s. Removing to reduce space.")
#		os.remove(storefile)

def triage(inputstore,data):
	chunksize = 100000
	storefolder = 'merged1' # this is in the h5 hierarchy
	outputstorename = data + "merged/merge-%s.h5" % os.getpid()
	max_str_bytes = 50

	errors = 0
	with pd.HDFStore(outputstorename, complevel=9, mode="a", complib='blosc') as outstore:
		with pd.HDFStore(inputstore, complevel=9, mode="r", complib='blosc') as store:
			row_size = store.get_storer('/tf/corpus').nrows
			storeiter = store.select('/tf/corpus', start=0, chunksize=chunksize)

			i = 0
			for chunk in storeiter:
				i += 1
				try:
					lang_groups = chunk.groupby(level=['language'])
					for lang,df in lang_groups:
						if df.empty:
							continue
						merged = df.groupby(level=['token']).sum()

						fname = "%s/%s" % (storefolder, lang)
						outstore.append(fname, merged, data_columns=['count'], min_itemsize = {'index': max_str_bytes})
					logging.info("Completed %d/%d" % (i, np.ceil(row_size/chunksize)))
				except:
					errors += 1
					logging.exception("Error processing batch %d (docs %d-%d) of input store" % (i, (i-1)*chunksize, i*chunksize))
				gc.collect()
	gc.collect()
	if errors == 0:
		return "success"
	else:
		return "%d errors on process %s, check logs" % (errors, os.getpid())

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


def listener(q):
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
				if result == "success":
					logging.info("Done processing batch %d" % i)
				else:
					logging.error(result)

def reduceCounts(data,core_count):
	init_log(data,"final")
	rawstores = glob.glob(data + "stores/*h5")

	manager = mp.Manager()
	q = manager.Queue()
	p = mp.Pool(int(core_count),initializer=init_log,initargs=(data,))

	logging.info("Processing Started")

	watcher = p.apply_async(listener, (q,))

	jobs = []
	for store in rawstores:
		job = p.apply_async(triage,(store,data))
		jobs.append(job)

	for job in tqdm(jobs):
		job.get()

	q.put('kill')

	sumTokenCounts(glob.glob(data + "merged/*.h5"),data)
	finalCombine(glob.glob(data + 'final/fromnodes*h5'),data)
	finalSort(data)

	with pd.HDFStore(data + 'final/final-sorted.h5') as store:
		keys = store.keys()
		sizes = [store.get_storer(key).shape for key in keys]
	print(pd.Series(sizes, index=keys).sort_values(ascending=False))
	print(pd.Series(sizes, index=keys).sort_values(ascending=False).shape)

#	q.put('kill')
	p.close()
	p.join()

if __name__ == "__main__":
	reduceCounts(sys.argv[1],sys.argv[2],sys.argv[3])