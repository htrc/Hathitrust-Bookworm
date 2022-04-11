import argparse, configparser, MySQLdb, csv
import multiprocessing as mp
from functools import partial
from tqdm import tqdm

cursor = None
def buildDBConnection(conargs):
	try:
		db = MySQLdb.connect(**conargs)
	except:
		raise

	global cursor
	cursor = db.cursor()

def callDatabase(wordid):
	cursor.execute("SELECT COUNT(1) FROM master_bookcounts WHERE wordid = %i;" %(wordid,))
	row = cursor.fetchone()
	return [wordid,row[0]]

def getVolumeCounts(args):
	config = configparser.ConfigParser()
	config.read(args.config_location)

	conargs = {
		"db": config['client']['database'],
		"use_unicode": 'True',
		"charset": 'utf8',
		"user": config['client']['user'],
		"password": config['client']['password'],
		"host": config['mysqld']['host'],
		"port": 3307
	}

	manager = mp.Manager()
	cores = int(args.core_count)
	pool = mp.Pool(cores,initializer=buildDBConnection,initargs=(conargs,),maxtasksperchild=100)

	result_list = []
	for result in tqdm(pool.imap(callDatabase,range(0,8271556),100)):
		result_list.append(result)

	with open("volumecounts_per_word.tsv",'w') as open_output_file:
		output_writer = csv.writer(open_output_file,delimiter='\t')
		for row in result_list:
			output_writer.writerow(row)

	pool.close()
	pool.join()

if __name__ == "__main__":
	parser = argparse.ArgumentParser()
	parser.add_argument("config_location", help="config location")
	parser.add_argument("core_count", help="core count")
	args = parser.parse_args()

	getVolumeCounts(args)