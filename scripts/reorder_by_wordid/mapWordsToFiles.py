import argparse, csv, math, json, sys

def mapWordsToFiles(args):
	mappings = {}
	available_file_number = 0
	combo_file_number = None
	combo_file_line_count = 0

	max_single = 6000000
	min_single = 4000000
	with open(args.volcounts,"r") as volcounts:
		volcountreader = csv.reader(volcounts,delimiter='\t')
		for row in volcountreader:
			if int(row[1]) > max_single:
				number_of_files = math.ceil(int(row[1])/max_single)
				mappings[row[0]] = []
				for i in range(0,number_of_files):
					mappings[row[0]].append(available_file_number)
					available_file_number += 1
			elif int(row[1]) < min_single:
				if combo_file_number is None:
					combo_file_number = available_file_number
					available_file_number += 1

				mappings[row[0]] = [combo_file_number]
				combo_file_line_count += int(row[1])

				if combo_file_line_count > max_single:
					combo_file_number = None
					combo_file_line_count = 0
			else:
				mappings[row[0]] = [available_file_number]
				available_file_number += 1

	with open("worid2file_mapping.json","w") as outfile:
		json.dump(mappings,outfile)

if __name__ == "__main__":
	parser = argparse.ArgumentParser()
	parser.add_argument("volcounts", help="volume counts per word TSV")
	args = parser.parse_args()

	mapWordsToFiles(args)