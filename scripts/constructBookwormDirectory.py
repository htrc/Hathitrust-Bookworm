import os, sys

if os.name == 'nt':
	SLASH = '\\'
else:
	SLASH = '/'

def constructBookwormDirectory(bookworm_directory):
	if bookworm_directory[-1:] != SLASH:
		bookworm_directory = bookworm_directory + SLASH

	os.mkdir(bookworm_directory + 'texts/')
	os.mkdir(bookworm_directory + 'texts/wordlist')
	os.mkdir(bookworm_directory + 'texts/encoded')
	os.mkdir(bookworm_directory + 'texts/encoded/unigrams')
	os.mkdir(bookworm_directory + 'texts/encoded/bigrams')

if __name__ == "__main__":
	constructBookwormDirectory(sys.argv[1])