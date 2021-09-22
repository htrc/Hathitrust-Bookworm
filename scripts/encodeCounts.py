import sys, os, json
import pandas as pd

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

#	print(output)
	return output

def encodeH5File(counts,word_dict,vol_dict,output_folder):
	store_iterator = pd.read_hdf(counts,key='/tf/docs',iterator=True,chunksize=1000000)

#	with open(output_folder + 'tmp-count.txt','a') as outfile:
	file_chunk_counter = 0
	for chunk in store_iterator:
#		print(type(chunk['count']))
#		print(chunk['count'].index.levels[0].values)
		print(chunk['count'].index.levels[0].values)
		chunk['count'].index.set_levels(applyEncoding(chunk['count'].index.levels[0].values,vol_dict),level=0,inplace=True)
#		chunk['count'].index.set_levels(applyEncoding(chunk['count'].index.levels[1].values,word_dict),level=1,inplace=True)

#		for i in range(0,len(chunk['count'].index.levels[0])):
#			try:
#				chunk['count'].index.levels[0][i] = vol_dict[chunk['count'].index.levels[0][i]]
#			except Exception as e:
#				print("Broke on " + chunk['count'].index.levels[0][i])
#				print(e)
		drop_list = []
#		encoded_word_mapping = {}
		encoded_index = []
#		unencoded_tuple = []
#		encoded_tuple = []
		for ind in chunk['count'].index.values:
#			if ind == 'mdp.39015018415946':
#				print(ind)
#				print(ind[0])
			try:
#				print(type(ind))
#				encoded_word_mapping[ind[1]] = word_dict[ind[1]]
				encoded_index.append((ind[0],word_dict[ind[1]]))
#				encoded_tuple.append((ind[0],word_dict[ind[1]]))
#				print(type((ind[0],word_dict[ind[1]])))
#				unencoded_tuple.append(ind)
#				for items_a, items_b in chunk['count'].items():
#					print(items_a, items_b)
			except:
				drop_list.append(ind)

#		print(chunk['count'].values)
#		print(type(chunk))
#		print(chunk.dtypes)
#		print(chunk.shape)
#		print(chunk.axes)
#		print(len(chunk['count']))
		chunk.drop(drop_list,inplace=True)
#		print(chunk['count'].replace(encoded_word_mapping))
		encoded_df = pd.DataFrame(data=chunk['count'].values,index=pd.MultiIndex.from_tuples(encoded_index))
#		print(encoded_df.values)
#		print(encoded_df.axes)
#		chunk.rename(columns=encoded_word_mapping,inplace=True)
#		print(len(chunk['count']))
#		print(chunk['count'].index.values)
#		print("Reached CSV write")

		encoded_df.to_csv(output_folder + 'tmp-count-' + str(file_chunk_counter) + '.txt',mode='a',header=False,sep='\t')
#		print(chunk[0])
		if os.stat(output_folder + 'tmp-count-' + str(file_chunk_counter) + '.txt').st_size > 100 * 1024 * 1024:
			file_chunk_counter = file_chunk_counter + 1


def encodeCounts(wordlist,volumelist,counts_folder,output_folder):
	if output_folder[-1:] != SLASH:
		output_folder = output_folder + SLASH

	with open(wordlist,'r') as wordlist_file:
		word_dict = json.load(wordlist_file)

	with open(volumelist,'r') as volumelist_file:
		vol_dict = json.load(volumelist_file)

	for file in os.listdir(counts_folder):
		if file.endswith('.h5'):
			encodeH5File(os.path.join(counts_folder,file),word_dict,vol_dict,output_folder)

if __name__ == "__main__":
	encodeCounts(sys.argv[1],sys.argv[2],sys.argv[3],sys.argv[4])