import argparse

import pandas as pd


def main(cliargs):
	file1_name = cliargs.first
	file2_name = cliargs.second

	file1_path = "../input/" + file1_name
	file2_path = "../input/" + file2_name

	print("Comparando el fichero", file1_path, "con el fichero", file2_path)

	file1 = pd.read_csv(file1_path, delimiter=";")
	file2 = pd.read_csv(file2_path, delimiter=";")

	print(file1.head())
	print(file2.head())

	for index, value in file1.iterrows():
		row = ""
		for column in value:
			row += str(column) + " "
		print(row)


if __name__ == '__main__':
	parser = argparse.ArgumentParser(
		description="csv comparator",
		epilog="An csv comparator. It checks if both csv have the same records.")

	parser.add_argument(
		"first",
		help="first csv",
		metavar="first-file")
	parser.add_argument(
		"second",
		help="second csv",
		metavar="second-file")

	args = parser.parse_args()
	main(args)
