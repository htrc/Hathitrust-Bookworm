import os, sys, time, datetime
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

def generateWordList(features,data,core_count):
	#check that features and data are valid locations. If not throw error
	if data[-1:] != SLASH:
		data = data + SLASH

	if features[-1:] != SLASH:
		features = features + SLASH

	start_time = datetime.datetime.now().time()

	if validateInputs(features,data,core_count):
		generateStores.generateStores(features,data,int(core_count))
		reduceCounts.reduceCounts(data,int(core_count))
		createWordlist.createWordlist(features,data,int(core_count))

	end_time = datetime.datetime.now().time()
	print("Start time: " + str(start_time))
	print("End time: " + str(end_time))
	print("Run duration: " + str(datetime.datetime.combine(datetime.date.min,end_time)-datetime.datetime.combine(datetime.date.min,start_time)))

if __name__ == "__main__":
	generateWordList(sys.argv[1],sys.argv[2],sys.argv[3])