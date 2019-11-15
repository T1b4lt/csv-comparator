import argparse
import time

import pandas as pd
from joblib import Parallel, delayed
from tqdm import tqdm


def fun(arg):
	if len(arg) == 1:
		return arg[0]


def main(cliargs):
	t0 = time.time()

	file1_name = cliargs.first
	file2_name = cliargs.second

	file1_path = "../input/" + file1_name
	file2_path = "../input/" + file2_name

	print("Comparando el fichero", file1_path, "con el fichero", file2_path)

	file1 = pd.read_csv(file1_path, delimiter=";")
	file2 = pd.read_csv(file2_path, delimiter=";")

	print(file1.head(5))
	print(file2.head(5))

	diff = pd.concat([file1, file2], sort=False)
	diff = diff.reset_index(drop=True)
	diff_gpby = diff.groupby(list(diff.columns))

	print("Collecting diffs...")
	idx = Parallel(n_jobs=-1, verbose=10, backend='multiprocessing')(map(delayed(fun), diff_gpby.groups.values()))
	idx = list(filter(None, idx))

	csv_content = []
	csv_header = []

	print("Generating output dataframe...")
	for i in tqdm(range(len(idx))):
		row = []
		for column_name in diff.columns:
			if i == 0:
				csv_header.append(column_name)
			row.append(diff[column_name][idx[i]])
		csv_content.append(row)

	print("Creating DataFrame to export to csv")
	df_to_export = pd.DataFrame(csv_content, columns=csv_header)
	print(df_to_export.head())
	print("Exporting...")
	df_to_export.to_csv("../output/diff_output.csv", sep=";", na_rep="NODATA", index=False)
	print("Finished!")
	print(time.time() - t0, "seconds of execution")


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
