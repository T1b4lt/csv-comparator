import argparse


def main(args):
	print("hola")


if __name__ == '__main__':
	parser = argparse.ArgumentParser(
		description="Excel comparator",
		epilog="An excel comparator. It checks if both excels have the same records.")

	"""
	parser.add_argument(
		"first",
		help="first excel",
		metavar="FST")
	parser.add_argument(
		"second",
		help="second excel",
		metavar="SEC")
	"""
	args = parser.parse_args()
	main(args)
