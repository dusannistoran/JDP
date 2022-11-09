"""
Once a day, you should run a job that will update data in the Country table original saved on Hive.
The Country table is a two columns table: CountryID and Country name.
"""

from countries import countries_all_ids_names
import csv

#data_edited_csv_filepath = '/home/dusan/PycharmProjects/JDP_Data_Engineering_Task/venv/csvs/data_edited2.csv'
data_edited_csv_filepath = 'file:///home/scala/src/main/resources/csvs/data_edited.csv'
#countries_csv_filepath = '/home/dusan/PycharmProjects/JDP_Data_Engineering_Task/venv/csvs/countries2.csv'
countries_csv_filepath = 'file:///home/scala/src/main/resources/csvs/countries.csv'

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

with open(f'{countries_csv_filepath}', 'w', encoding="UTF-8", newline='') as csv_file:
    writer = csv.writer(csv_file)
    writer.writerow(data_output_countries_dict.keys())
    writer.writerows(zip(*data_output_countries_dict.values()))



