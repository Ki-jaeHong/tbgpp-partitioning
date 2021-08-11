import csv
import os
import sys


def convert():
	
	# Configuration
	#target = "P2P" # P2P/P2C
	target = sys.argv[1]
	assert target == "P2P" or target == "P2C"

	# Input
	#input_filedir = "/home/tslee/ldbc_snb_datagen/out/csv/interactive/composite-projected-fk/dynamic/Person_knows_Person/"
	#input_filename = "part-00000-2b835fb1-4b41-4fbc-a890-4969a25e7d28-c000.csv"
	#input_filepath = input_filedir + input_filename
	input_filepath = sys.argv[2]

	# Output
	#output_filedir = "/home/tslee/jhko/tbgpp-partitioning/ldbccsv-tbgpp-converter/tmp/"
	#output_filepath = output_filedir + target
	output_filepath = sys.argv[3]

	# Validate input / output
	if not os.path.isfile(input_filepath):
		print("No input file")
		exit(1)
	if not os.path.exists(os.path.dirname(output_filepath)):
		print("Output dir does not exist")
		exit(1)
	if os.path.isfile(output_filepath):
		print("Output file already exists")
		exit(1)

	readcnt = 0
	writecnt = 0
	with open(input_filepath, 'r', newline='') as f:
		with open(output_filepath, 'w', newline='') as fout:
			reader = csv.reader(f, delimiter='|')
			writer = csv.writer(fout, delimiter='\t')

			for idx, row in enumerate(reader):
				readcnt = idx+1
				# if idx == 0 :	# del first row
				# 	continue

				if target == "P2P":
					assert( len(row) == 3 )
					writer.writerow([ str(row[1]), str(row[2]) ])
					writecnt += 1

				if target == "P2C":
					assert( len(row) == 3 )
					writer.writerow( [ str(row[1]), str(row[2]) ])
					writecnt += 1
		
		fout.close()
	f.close()

	assert readcnt == writecnt + 1 , "Wrong size written"

if __name__ == "__main__":
	assert len(sys.argv) == 4
	convert()
	exit(0)