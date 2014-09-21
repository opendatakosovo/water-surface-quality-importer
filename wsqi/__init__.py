
import csv
import datetime
from bson import ObjectId
from pymongo import MongoClient
from slugify import slugify
from termcolor import colored


mongo = MongoClient()
water_surface_quality_collection = mongo.kepa.watersurfacequality

# Clear collections before running importer
water_surface_quality_collection.remove({})


def import_data(river_name, csv_filepath):
    data_table = create_data_table(csv_filepath)
    docs = create_water_surface_quality_documents(data_table)
    persist(docs)

    return len(docs)


def persist(docs):
    for doc in docs:
        water_surface_quality_collection.insert(doc)


def create_data_table(csv_filepath):
    data_table = []

    with open(csv_filepath, 'rb') as csvfile:
        reader = csv.reader(csvfile)
        index = 0
        for row in reader:
            data_table.append(row)
            index = index + 1
    return data_table


def create_water_surface_quality_documents(data_table):
    docs = []

    num_of_columns = len(data_table[0])
    river_name = ''
    station_code = ''
    sample_site = ''

    for column_index in range(4, num_of_columns):

        date = data_table[7][column_index]
        if date != '':

            doc_dict = {}
            doc_dict['_id'] = str(ObjectId())

            for row_index in range(3, len(data_table)):

                cell_value = data_table[row_index][column_index]

                # River name
                if row_index == 3:
                    if cell_value != '':
                        river_name = cell_value

                        print_text = "\nImporting sample data from river '" + river_name + "'"
                        print colored(print_text, 'green', attrs=['bold'])

                    doc_dict['lumi'] = {
                        'emri': river_name,
                        'slug': slugify(river_name)
                    }

                # Station code
                if row_index == 4:
                    if cell_value != '':
                        station_code = cell_value

                        print_text = "\nSamples from station " + colored(station_code, 'red', attrs=['bold'])
                        print print_text,

                    doc_dict['stacion'] = get_station_info(station_code)

                # Sampling site
                elif row_index == 5:
                    if cell_value != '':
                        sample_site = cell_value
                        print "(%s):" % sample_site

                    doc_dict['vendmostrimi'] = {
                        'emri': sample_site,
                        'slug': slugify(sample_site)
                    }

                # Sampling date
                elif row_index == 7:
                    sampling_date = cell_value
                    sampling_time = data_table[10][column_index]

                    sampling_date_time = sampling_date + '.' + sampling_time

                    sampling_date = datetime.datetime.strptime(sampling_date_time, '%d.%m.%Y.%H:%M')
                    print "         %s at %s" % (cell_value, sampling_time)

                    doc_dict['data'] = sampling_date

                # Measured Parameters
                elif row_index >= 11:

                    value = data_table[row_index][column_index]

                    if value != '' and value != 0:
                        parameter = data_table[row_index][1]
                        parameter_json_key = to_camel_case(parameter)

                        symbol = data_table[row_index][2]
                        unit = data_table[row_index][3]

                        doc_dict[parameter_json_key] = {
                            'value': value,
                            'simboli': symbol,
                            'njesia': unit
                        }

            docs.append(doc_dict)

    return docs


def get_station_info(station_code):
    return {
        'kodi': station_code,
        'emri': 'yo',
        'slug': slugify('yo'),
        'coordinates': {
            'lon': -1,
            'lat': -1
        },
        'riverBasin': {
            'emri': '',
            'slug': ''
        },
        'seaRegion': {
            'emri': '',
            'slug': ''
        },
        'catchmentArea': -1,
        'populationDensity': -1,
        'altitude': -1
    }


def to_camel_case(term):
    term = term.strip()
    words = term.split(' ')

    camel_words = [capitalize_first_letter(w) for w in words if len(w) > 1]

    joined_camel_words = ''.join(camel_words)
    cameled_term = joined_camel_words[0].lower() + joined_camel_words[1:]

    return cameled_term


def capitalize_first_letter(word):
    word = slugify(word).replace('-', '')
    return "" + word[0].upper() + word[1:]
