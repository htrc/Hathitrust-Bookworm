import glob
import re
import pandas as pd
import numpy as np
import multiprocessing as mp
import logging
import sys
import os
import gc
import random
import ctypes
from tqdm import tqdm # Progress bars!
import dask.dataframe as dd
from dask.diagnostics import ProgressBar, Profiler, ResourceProfiler, CacheProfiler, visualize
import time, datetime
from datetime import timedelta
import psutil

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

def token_sum_listener(q,savestore,max_str_bytes):
	i = 0

	while(1):
		results = q.get()

		if results:
#			print("Got Results")
#			print(os.getpid())
			if results == 'kill':
				print("Results say kill")
				break
			else:
				if 'lang' in results and 'full_merge' in results:
					print("Writing %s counts to %s - Started" % (results['lang'], savestore))
					queue_size = q.qsize()
					index_command = False
					if queue_size == 0:
						index_command = True
					try:
						with pd.HDFStore(savestore, complevel=9, mode="a", complib='blosc') as store:
							store.append(results['lang'],results['full_merge'],data_columns=['count'],min_itemsize = {'index': max_str_bytes},index=index_command)
					except Exception as e:
						print(e)
						sys.exit()
					print("Writing %s counts to %s - Finished - Remaining queue: %s" % (results['lang'], savestore,queue_size))
					gc.collect()
				else:
					logging.error(results)

def sumTokenCounts(storefile,chunksize,batch_limit,q,big_langs=False):
	big_languages = ['eng', 'ger', 'fre', 'lat', 'rus', 'jpn', 'ita', 'spa']
	print(storefile)
	logging.info("Next store: %s" % storefile)
	try:
		# Get Unique languages
		with pd.HDFStore(storefile, complevel=9, mode="r", complib='blosc') as store:
			langs = set([key.split("/", maxsplit=-1)[-1] for key in store.keys() if 'merged1' in key])

		if not big_langs:
			for bigl in big_languages:
				try:
					langs.remove(bigl)
				except:
					logging.info("Tried to remove %s from language list, but it already wasn't in %s" % (bigl,storefile))
		else:
			big_lang_set = []
			for bigl in big_languages:
				if bigl in langs:
					big_lang_set.append(bigl)

			langs = set(big_lang_set)

		for lang in langs:
			batch = False
			logging.info("Starting lang %s from %s" % (lang, storefile))
#			print(lang)

			if not re.match('[a-z]{3}', lang):
				logging.error("lang '%s' is not three alphanumeric characters. Skipping for now. (%s)" % (lang, storefile))
				continue

			memory_threshold = 90.0

			while(psutil.virtual_memory().percent > memory_threshold):
				logging.info("Memory usage too high. Usage at %d. Taking a short nap to relieve some pressure." % psutil.virtual_memory().percent)
				time.sleep(3 * 60)

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
				print("Memory usage %d" % psutil.virtual_memory().percent)
				while(psutil.virtual_memory().percent > memory_threshold):
						logging.info("Memory usage too high. Usage at %d. Taking a short nap to relieve some pressure." % psutil.virtual_memory().percent)
						time.sleep(3 * 60)
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

					print("%s - %s : Starting full merge with %d partitions" % (storefile, lang, ddf.npartitions))
					local_start_time = datetime.datetime.now().time()

					full_merge = ddf.reset_index().groupby('token').sum().compute()

					local_end_time = datetime.datetime.now().time()
					duration = datetime.datetime.combine(datetime.date.min,local_end_time)-datetime.datetime.combine(datetime.date.min,local_start_time)
					if duration < timedelta(0):
						duration = (timedelta(days=1) + duration)
					print("%s - %s : Finished full merge with %d partitions – %s" % (storefile, lang, ddf.npartitions, duration))
					#if lang == 'eng':
						# For curiosity: see the profiling for English
					#    prof.visualize()
					logging.info("Success! Saving merged.")
					# The /fromnodes table is the sum from all the different stores, but will need to be summed one more time
	#				with pd.HDFStore(savestore, complevel=9, mode="a", complib='blosc') as store:
	#					store.append(lang,full_merge,data_columns=['count'],min_itemsize = {'index': max_str_bytes})
#					print("Memory usage %d" % psutil.virtual_memory().percent)
#					while(psutil.virtual_memory().percent > memory_threshold):
#						logging.info("Memory usage too high. Usage at %d. Taking a short nap to relieve some pressure." % psutil.virtual_memory().percent)
#						time.sleep(3 * 60)
					q.put({ 'lang': lang, 'full_merge': full_merge })
#					print("Memory usage %d" % psutil.virtual_memory().percent)
#					while(psutil.virtual_memory().percent > memory_threshold):
#						logging.info("Memory usage too high. Usage at %d. Taking a short nap to relieve some pressure." % psutil.virtual_memory().percent)
#						time.sleep(3 * 60)
				except Exception as e:
					if e == BrokenPipeError:
						print(e)
						sys.exit()
					logging.exception("Can't compute or save lang for %s in %s" % (lang, storefile))

				gc.collect()

				if batch == False:
					break

			gc.collect()
	except:
		logging.exception("Can't read languages from %s" % storefile)

#	logging.info("Finished processing %s. Removing to reduce space.")
#	os.remove(storefile)

def triage(inputstore,data,q):
	chunksize = 100000
	storefolder = 'merged1' # this is in the h5 hierarchy
	outputstorename = data + "merged/merge-%s.h5" % inputstore[inputstore.rfind('_')+1:-3]
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
		q.put(inputstore)
		return inputstore
	else:
		q.put("%d errors on process %s, check logs" % (errors, os.getpid()))
		return "%d errors on process %s, check logs" % (errors, os.getpid())

def remove_incomplete_merges(rawstores,mergedstores,data):
	for store in rawstores:
		checkfile = data + 'merged/merge-' + store[store.rfind('_')+1:]
		if checkfile in mergedstores:
			print("Removing incomplete file %s" % checkfile)
			os.remove(checkfile)

def get_unprocessed_stores(rawstores,successfile):
	try:
		with open(successfile, "r") as f:
			paths = f.read().strip().split("\n")
		return np.setdiff1d(rawstores,paths,True)
	except:
		return rawstores

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
	while(1):
		results = q.get()

		if results:
#			print("Got Results")
#			print(os.getpid())
			if results == 'kill':
#				print("Results say kill")
				break
			else:
				if results[-3:] == ".h5":
					with open(successfile,'a') as f:
						f.write(results + "\n")
					logging.info("Done processing batch %s" % results)
				else:
					logging.error(results)

def reduceCounts(data,core_count):
	init_log(data,"final")
	successfile = data + "successful-merges.txt"
	rawstores = glob.glob(data + "stores/*h5")
	rawstores = get_unprocessed_stores(rawstores,successfile)
	remove_incomplete_merges(rawstores,glob.glob(data + "merged/*h5"),data)

	manager = mp.Manager()
	q = manager.Queue()
	p = mp.Pool(int(core_count),initializer=init_log,initargs=(data,),maxtasksperchild=1)

	logging.info("Processing Started")

	watcher = p.apply_async(listener, (q,successfile))

	jobs = []
	for store in rawstores:
		job = p.apply_async(triage,(store,data,q))
		jobs.append(job)

	for job in tqdm(jobs):
		job.get()

	q.put('kill')
	p.close()
	p.join()

	p = mp.Pool(int(core_count),initializer=init_log,initargs=(data,))

	stores = glob.glob(data + "merged/*.h5")
	max_str_bytes = 50
	chunksize = 100000
#	batch_limit = 2*10**7
	#It runs much faster when large languages aren't split into batches. Batches also don't seem to reduce memory usage, so the big batch_limit value is the clear winner here
	batch_limit = 6*10**8
	savestore = data + "final/fromnodes-323.h5"

	watcher = p.apply_async(token_sum_listener, (q,savestore,max_str_bytes))
	sum_jobs = []
	for storefile in stores:
		sum_job = p.apply_async(sumTokenCounts,(storefile,chunksize,batch_limit,q))
		sum_jobs.append(sum_job)

	for sum_job in sum_jobs:
		sum_job.get()

	print("Token summing complete")
	while(q.qsize() > 0):
		print("Waiting to write %s language sums to %s" % (q.qsize(),savestore))
		time.sleep(60)

	last_write_unfinished = True
	while(last_write_unfinished):
		try:
			with pd.HDFStore(savestore, complevel=9, mode="a", complib='blosc') as store:
				last_write_unfinished = False
		except:
			print("Waiting for final write to %s to finish" % savestore)
			time.sleep(60)

	q.put('kill')
	p.close()
	p.join()

	p = mp.Pool(4,initializer=init_log,initargs=(data,))
	watcher = p.apply_async(token_sum_listener, (q,savestore,max_str_bytes))

	big_sum_jobs = []
	for storefile in stores:
		big_sum_job = p.apply_async(sumTokenCounts,(storefile,chunksize,batch_limit,q,True))
		big_sum_jobs.append(big_sum_job)

	for big_sum_job in big_sum_jobs:
		big_sum_job.get()

	print("Token summing complete")
	while(q.qsize() > 0):
		print("Waiting to write %s language sums to %s" % (q.qsize(),savestore))
		time.sleep(60)

	last_write_unfinished = True
	while(last_write_unfinished):
		try:
			with pd.HDFStore(savestore, complevel=9, mode="a", complib='blosc') as store:
				last_write_unfinished = False
		except:
			print("Waiting for final write to %s to finish" % savestore)
			time.sleep(60)

	q.put('kill')
	p.close()
	p.join()

#	sumTokenCounts(glob.glob(data + "merged/*.h5"),data)
	finalCombine(glob.glob(data + 'final/fromnodes*h5'),data)
	finalSort(data)

	with pd.HDFStore(data + 'final/final-sorted.h5') as store:
		keys = store.keys()
		sizes = [store.get_storer(key).shape for key in keys]
	print(pd.Series(sizes, index=keys).sort_values(ascending=False))
	print(pd.Series(sizes, index=keys).sort_values(ascending=False).shape)

#	q.put('kill')
#	p.close()
#	p.join()

if __name__ == "__main__":
	reduceCounts(sys.argv[1],sys.argv[2])