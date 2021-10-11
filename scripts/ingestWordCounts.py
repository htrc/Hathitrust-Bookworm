import os, sys, subprocess, argparse

if os.name == 'nt':
	SLASH = '\\'
else:
	SLASH = '/'

def validateInput(input_folder):
	if input_folder[-1:] != SLASH:
		input_folder = input_folder + SLASH

	if input_folder[-1 * len(".bookworm/"):] != ".bookworm/":
		print("The target folder is incorrect. It must be named '.bookworm'")
		return False

	if os.path.isdir(input_folder + 'texts/encoded/unigrams'):
		working_folder = input_folder + 'texts/encoded/unigrams'
		working_folder_contents = os.listdir(working_folder)
		if '.DS_Store' in working_folder_contents:
			working_folder_contents.remove('.DS_Store')

		confirmed_dirs = [ content for content in working_folder_contents if os.path.isdir(os.path.join(working_folder,content)) ]
		if set(working_folder_contents) == set(confirmed_dirs):
			print("Input folder formatted correctly")
			return working_folder
		else:
			print("Please remove any files from %s that are not the subdirectories" % working_folder)
			return False
	else:
		print(".bookworm is not formatted correctly: .bookworm/texts/encoded/unigrams must exist.")
		return False

#This assumes there are multiple directoreis in .bookworm/texts/encoded/unigrams that each contain multiple text files that store the encoded word counts
def ingestWordCounts(args):
	working_folder = validateInput(args.target_folder)
	input_folder = args.target_folder
	if input_folder[-1:] != SLASH:
		input_folder = input_folder + SLASH
	completed_folder = input_folder + 'texts/encoded/unigrams'

	if working_folder:
		subdirectories = os.listdir(working_folder)
		if '.DS_Store' in subdirectories:
			subdirectories.remove('.DS_Store')

		first = True
		for subdirectory in subdirectories:
			print(subdirectory)
			subprocess.run("mv " + os.path.join(working_folder,subdirectory) + "/*.txt " + working_folder,shell=True)

#			ingest_command = ["bookworm","-l","debug","prep","database_wordcounts","--no-delete"]
			ingest_command = ["bookworm","-l","debug","prep","database_wordcounts","--no-delete"]
			if first:
				ingest_command = ingest_command[:-1]
				first = False
#			print(ingest_command)
			results = subprocess.call(ingest_command)
			subprocess.run("mv " + working_folder + "/*.txt " + os.path.join(working_folder,subdirectory),shell=True)
			subprocess.run("mv " + subdirectory + " " + completed_folder,shell=True)
	else:
		sys.exit()

if __name__ == "__main__":
	parser = argparse.ArgumentParser()
	parser.add_argument("target_folder", help="The location of the .bookworm folder that the word counts are stored in")
	args = parser.parse_args()
	ingestWordCounts(args)