import sys, os, json, argparse
import pandas as pd
import multiprocessing as mp
from tqdm import tqdm

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

def processChunk(chunk,word_dict,vol_dict,output_folder,store_name):
#	print("Processing a chunk of %i volumes" % len(chunk['count'].index.levels[0].values))
	chunk['count'].index.set_levels(applyEncoding(chunk['count'].index.levels[0].values,vol_dict),level=0,inplace=True)

	drop_list = []
	encoded_index = []
	for ind in chunk['count'].index.values:
		try:
			encoded_index.append((ind[0],word_dict[ind[1]]))
		except:
			drop_list.append(ind)

	chunk.drop(drop_list,inplace=True)
	encoded_df = pd.DataFrame(data=chunk['count'].values,index=pd.MultiIndex.from_tuples(encoded_index))

	encoded_df.to_csv(output_folder + store_name + SLASH + 'tmp-count-' + str(os.getpid()) + '.txt',mode='a',header=False,sep='\t')

def parallelEncodeH5File(core_count,counts,word_dict,vol_dict,output_folder):
	store_iterator = pd.read_hdf(counts,key='/tf/docs',iterator=True,chunksize=1000000)
	store_name = counts[counts.rfind('/')+1:-3]
	os.mkdir(output_folder + store_name)

	with mp.Pool(processes=int(core_count)) as pool:
		jobs = []
		
		file_chunk_counter = 0
		for chunk in store_iterator:
			job = pool.apply_async(processChunk,(chunk,word_dict,vol_dict,output_folder,store_name))
			jobs.append(job)

		for job in jobs:
			job.get()


def encodeH5File(counts,word_dict,vol_dict,output_folder):
	store_iterator = pd.read_hdf(counts,key='/tf/docs',iterator=True,chunksize=1000000)
	store_name = counts[counts.rfind('/')+1:-3]
	os.mkdir(output_folder + store_name)

	file_chunk_counter = 0
	for chunk in store_iterator:
#		print("Processing a chunk of %i volumes" % len(chunk['count'].index.levels[0].values))
		chunk['count'].index.set_levels(applyEncoding(chunk['count'].index.levels[0].values,vol_dict),level=0,inplace=True)

		drop_list = []
		encoded_index = []
		for ind in chunk['count'].index.values:
			try:
				encoded_index.append((ind[0],word_dict[ind[1]]))
			except:
				drop_list.append(ind)

		chunk.drop(drop_list,inplace=True)
		encoded_df = pd.DataFrame(data=chunk['count'].values,index=pd.MultiIndex.from_tuples(encoded_index))

		encoded_df.to_csv(output_folder + store_name + SLASH + 'tmp-count-' + str(file_chunk_counter) + '.txt',mode='a',header=False,sep='\t')
		if os.stat(output_folder + store_name + SLASH + 'tmp-count-' + str(file_chunk_counter) + '.txt').st_size > 100 * 1024 * 1024:
			file_chunk_counter = file_chunk_counter + 1


def encodeCounts(args):
	if args.output_folder[-1:] != SLASH:
		args.output_folder = args.output_folder + SLASH

	with open(args.wordlist,'r') as wordlist_file:
		word_dict = json.load(wordlist_file)

	with open(args.volumelist,'r') as volumelist_file:
		vol_dict = json.load(volumelist_file)

	if args.size_limit:
		print("Each .h5 file will be turned into an indeterminate number of files with a max size")
		with mp.Pool(processes=int(args.core_count)) as pool:
			jobs = []

			for file in os.listdir(args.counts_folder):
				if file.endswith('.h5'):
					job = pool.apply_async(encodeH5File,(os.path.join(args.counts_folder,file),word_dict,vol_dict,args.output_folder))
					jobs.append(job)

			for job in tqdm(jobs):
				job.get()
	else:
		print("Each .h5 file will be turned into %s output files of indeterminate size" % args.core_count)
		for file in tqdm(os.listdir(args.counts_folder)):
			if file.endswith('.h5'):
				parallelEncodeH5File(args.core_count,os.path.join(args.counts_folder,file),word_dict,vol_dict,args.output_folder)

if __name__ == "__main__":
	parser = argparse.ArgumentParser()
	parser.add_argument("wordlist", help="The wordlist.json file that encodes words")
	parser.add_argument("volumelist", help="The volumelist.json file that encodes volumes")
	parser.add_argument("counts_folder", help="The folder that holds the .h5 files to count")
	parser.add_argument("output_folder", help="Foler to write the output to. Must exists")
	parser.add_argument("core_count", help="Number of cores you want to devote to the process")
	parser.add_argument("--size_limit", action="store_true", help="If this flag is turned on each .h5 file will be split into a number of mappings that max out at around 100MB")
	args = parser.parse_args()
	
	encodeCounts(args)

#	if len(sys.argv) > 6:
#		encodeCounts(sys.argv[1],sys.argv[2],sys.argv[3],sys.argv[4],sys.argv[5],sys.argv[6])
#	else:
#		encodeCounts(sys.argv[1],sys.argv[2],sys.argv[3],sys.argv[4],sys.argv[5])