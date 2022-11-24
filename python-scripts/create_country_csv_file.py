from countries import countries_all_ids_names
import csv
import configparser

config = configparser.ConfigParser()
config.read('config.ini')

data_edited_csv_filepath = config['data_edited']['path']
countries_csv_filepath = config['countries']['path']

try:
    data_countries_list = []

    with open(f'{data_edited_csv_filepath}', 'r', encoding="ISO-8859-1") as csv_file:
        csv_reader = csv.DictReader(csv_file, delimiter=',')
        for line in csv_reader:
            data_countries_list.append(line["Country"])

    data_output_countries_dict = {
        "CountryID": [],
        "CountryName": []
    }

    for country_str in data_countries_list:
        #if country_str != 'invalid':
        country_strings = country_str.split('-')
        country_id = int(country_strings[0])
        country_name = countries_all_ids_names[country_id]
        data_output_countries_dict["CountryID"].append(str(country_id))
        data_output_countries_dict["CountryName"].append(country_name)
        #else:
        #    data_output_countries_dict["CountryID"].append('9999')
        #    data_output_countries_dict["CountryName"].append('invalid')

    print('data_output_countries_dict["CountryID"] length:', len(data_output_countries_dict["CountryID"]))
    print('data_output_countries_dict["CountryName"] length:', len(data_output_countries_dict["CountryName"]))
except:
    print("An exception occurred when opening input file and creating the countries dictionary.")

try:
    with open(f'{countries_csv_filepath}', 'w', encoding="UTF-8", newline='') as csv_file:
        writer = csv.writer(csv_file)
        writer.writerow(data_output_countries_dict.keys())
        writer.writerows(zip(*data_output_countries_dict.values()))
except:
    print("An exception occurred when writing countries dictionary into output file.")




