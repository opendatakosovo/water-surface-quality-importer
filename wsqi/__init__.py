
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
    station_csv_filepath = 'data/Sampling Station Info.csv'
    station_data_dict= create_station_data_dict(station_csv_filepath)

    water_quality_data_table = create_water_quality_data_table(csv_filepath)
    docs = create_water_quality_documents(water_quality_data_table, station_data_dict)
    persist(docs)

    return len(docs)


def create_station_data_dict(csv_filepath):
    data_dict = {}

    with open(csv_filepath, 'rb') as csvfile:
        reader = csv.reader(csvfile)

        # Skip the header
        next(reader, None)

        for row in reader:
            station_code = row[0]

            data_dict[station_code] = {
                'name': row[1],
                'longitude': row[2],
                'latitude': row[3],
                'riverBasin': row[4],
                'river': row[5],
                'seaRegion': row[6],
                'catchmentArea': row[7],
                'populationDensity': row[8],
                'altitude': row[9]
            }

    return data_dict


def persist(docs):
    for doc in docs:
        water_surface_quality_collection.insert(doc)


def create_water_quality_data_table(csv_filepath):
    data_table = []

    with open(csv_filepath, 'rb') as csvfile:
        reader = csv.reader(csvfile)
        for row in reader:
            data_table.append(row)
    return data_table


def create_water_quality_documents(water_quality_data_table, station_data_dict):
    docs = []

    num_of_columns = len(water_quality_data_table[0])
    river_name = ''
    station_code = ''
    sample_site = ''

    for column_index in range(4, num_of_columns):

        date = water_quality_data_table[7][column_index]
        if date != '':

            doc_dict = {}
            doc_dict['_id'] = str(ObjectId())

            for row_index in range(3, len(water_quality_data_table)):

                cell_value = water_quality_data_table[row_index][column_index]

                # River name
                if row_index == 3:
                    if cell_value != '':
                        river_name = cell_value

                        print_text = "\nImporting sample data from river '" + river_name + "'"
                        print colored(print_text, 'green', attrs=['bold'])

                    # This data is already in the station info object. We don't need it here.
                    '''
                    doc_dict['lumi'] = {
                        'emri': river_name,
                        'slug': slugify(river_name)
                    }
                    '''

                # Station code
                if row_index == 4:
                    if cell_value != '':
                        station_code = cell_value

                        print_text = "\nSamples from station " + colored(station_code, 'red', attrs=['bold'])
                        print print_text,

                    doc_dict['stacion'] = get_station_info(station_code, station_data_dict)

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
                    sampling_time = water_quality_data_table[10][column_index]

                    sampling_date_time = sampling_date + '.' + sampling_time

                    sampling_date = datetime.datetime.strptime(sampling_date_time, '%d.%m.%Y.%H:%M')
                    print "         %s at %s" % (cell_value, sampling_time)

                    doc_dict['data'] = sampling_date

                # Measured Parameters
                elif row_index >= 11:

                    value = water_quality_data_table[row_index][column_index]

                    if value != '' and value != 0:
                        parameter = water_quality_data_table[row_index][1].strip()
                        parameter_json_key = to_camel_case(parameter)

                        symbol = water_quality_data_table[row_index][2]
                        unit = water_quality_data_table[row_index][3]

                        doc_dict[parameter_json_key] = {}

                        if parameter not in ['Moti', 'Aroma', 'Ngjyra']:
                            doc_dict[parameter_json_key]['vlere'] = float(value)
                        else:
                            doc_dict[parameter_json_key]['vlere'] = value

                        doc_dict[parameter_json_key]['simboli'] = symbol
                        doc_dict[parameter_json_key]['njesia'] = unit

            docs.append(doc_dict)

    return docs


def get_station_info(station_code, station_data_dict):
    return {
        'kodi': station_code,
        'emri': station_data_dict[station_code]['name'],
        'slug': slugify(station_data_dict[station_code]['name']),
        'coordinates': {
            'gjatesi': float(station_data_dict[station_code]['longitude']),
            'gjeresi': float(station_data_dict[station_code]['latitude'])
        },
        'gjiriLumit': {
            'emri': station_data_dict[station_code]['riverBasin'],
            'slug': slugify(station_data_dict[station_code]['riverBasin'])
        },
        'lumi': {
            'emri': station_data_dict[station_code]['river'],
            'slug': slugify(station_data_dict[station_code]['river'])
        },
        'regjioniDetit': {
            'emri': station_data_dict[station_code]['seaRegion'],
            'slug': slugify(station_data_dict[station_code]['seaRegion'])
        },
        'vendMostrimi': float(station_data_dict[station_code]['catchmentArea']),
        'dendesiaPopullates': float(station_data_dict[station_code]['populationDensity']),
        'lartesia': int(station_data_dict[station_code]['altitude'])
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
