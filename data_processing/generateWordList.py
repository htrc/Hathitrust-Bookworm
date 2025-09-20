import os, sys, time, datetime, argparse
import generateStores
import reduceCounts
import createWordlist
from datetime import timedelta

if os.name == 'nt':
	SLASH = '\\'
else:
	SLASH = '/'

def validateInputs(features,data,core_count):
	if os.path.exists(features):
		if os.path.isdir(features):
			subfolders = [ 'final', 'logs', 'merged', 'stores' ]

			if os.path.exists(data) and os.path.isdir(data):
				for subfolder in subfolders:
					if not os.path.exists(data + SLASH + subfolder):
						os.mkdir(data + SLASH + subfolder)
			else:
				os.mkdir(data)
				for subfolder in subfolders:
					os.mkdir(data + SLASH + subfolder)

			try:
				core_count = int(core_count)
				return True
			except Exception as e:
				print("%s is not an integer. Please enter an integer number of cores to run" % core_count)
				return False

		else:
			print('%s is not a directory' % features)
			return False
	else:
		print('%s does not exist' % features)
		return False

def generateWordList(args):
	#check that volumes and output are valid locations. If not throw error
	if args.output[-1:] != SLASH:
		args.output = args.output + SLASH

	if args.volumes[-1:] != SLASH:
		args.volumes = args.volumes + SLASH

	start_time = datetime.datetime.now()

	if validateInputs(args.volumes,args.output,args.core_count):
		generateStores.generateStores(args.volumes,args.output,int(args.core_count))
		if not args.fast:
			reduceCounts.reduceCounts(args.output,int(args.core_count))
			createWordlist.createWordlist(args.volumes,args.output,int(args.core_count))

	end_time = datetime.datetime.now()
	print(f"Start time: {start_time}")
	print(f"End time: {end_time}")
	print(f"Run duration: {end_time-start_time}")

if __name__ == "__main__":
	parser = argparse.ArgumentParser()
	parser.add_argument("volumes", help="Where you rsync'd the EF files to, and where volumes/listing contains a file called ids.txt that is a copy of htids_hathifile_set_stubby.csv")
	parser.add_argument("output", help="Directory to write the output to")
	parser.add_argument("core_count", help="Number of cores to use for parallel processing")
	parser.add_argument("-f", "--fast", action="store_true", help="Generate stores for database creation, but don't build new wordlist. For relying on existing wordlist.")
	args = parser.parse_args()

	generateWordList(args)