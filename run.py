import os
import argparse
from wsqi import import_data
from termcolor import colored

#from mpadi import build_medians_collection

parser = argparse.ArgumentParser()
parser.add_argument(
    '--csvDirectoryPath',
    type=str, default='data/2013',
    help='The CSV directory path.')

args = parser.parse_args()
csv_directory_path = args.csvDirectoryPath


# Run the app
if __name__ == '__main__':
    result_dict = {}

    if os.path.isdir(csv_directory_path):
        for filename in os.listdir(csv_directory_path):
            if(filename.endswith(".csv")):

                csv_filepath = csv_directory_path + '/' + filename
                river_name = filename.replace('.csv', '')

                doc_count = import_data(river_name, csv_filepath)

                result_dict[river_name] = doc_count

    print colored("\n\nIMPORT SUMMARY:", 'magenta', attrs=['bold'])
    for key in result_dict.keys():
        print " - %s water surface quality monitoring documents created for '%s'." % (result_dict[key], key)

    print  # Just skip line
