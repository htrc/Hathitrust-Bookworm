import sys, os, json

if os.name == 'nt':
	SLASH = '\\'
else:
	SLASH = '/'

#Take wordlist text file that will be used for building bookworm and convert it to JSON where 
#the word is the key and the value is the numreical id we encode it as
def encodeWordlist(wordlist,output_folder):
	if output_folder[-1:] != SLASH:
		output_folder = output_folder + SLASH

	word_encodings = {}
	with open(wordlist,'r') as wordlist_text:
		line = wordlist_text.readline()
		while line:
			data = line.split('\t')
			word_encodings[data[1]] = data[0]
			line = wordlist_text.readline()

	with open(output_folder + 'wordlist.json','w') as outfile:
		json.dump(word_encodings,outfile)

if __name__ == "__main__":
	encodeWordlist(sys.argv[1],sys.argv[2])