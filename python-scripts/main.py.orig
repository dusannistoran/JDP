from countries import countries_invalid, countries_all
import csv
import random
import configparser

config = configparser.ConfigParser()
config.read('config.ini')

data_origin_csv_filepath = config['data_origin']['path']
data_edited_csv_filepath = config['data_edited']['path']

with open(data_origin_csv_filepath, 'r', encoding="ISO-8859-1") as file:
    data = [tuple(line) for line in csv.reader(file)]

countries_list = []

for tup in data:
    countries_list.append(tup[7])

one_to_six_list = list(range(1, 7))
new_invoice_nos = []
rand_num = random.choices(one_to_six_list, k=6)[0]

# skip the header
data = data[1:]

for idx, tup in enumerate(data):
    print('Index: ' + str(idx) + ' Country: ' + tup[7])
    country = tup[7]
    if country in countries_invalid:
        #data_first_fifty[idx] = tup, 'invalid'
        data[idx] = tup, 'invalid'
    else:
        #invoice_no = data_first_fifty[idx][0]
        invoice_no = data[idx][0]
        print('invoice_no: ' + invoice_no)
        country_id = countries_all[country]
        if invoice_no not in new_invoice_nos:
            rand_num = random.choices(one_to_six_list, k=6)[0]
            #data_first_fifty[idx] = tup, str(country_id) + '-' + str(rand_num)
            data[idx] = tup, str(country_id) + '-' + str(rand_num)
            new_invoice_nos.append(invoice_no)
            print('idx:', idx)
        else:
            #data_first_fifty[idx] = tup, str(country_id) + '-' + str(rand_num)
            data[idx] = tup, str(country_id) + '-' + str(rand_num)
            print('idx:', idx)

header = "InvoiceNo,StockCode,Description,Quantity,InvoiceDate,UnitPrice,CustomerID,Country"
with open(f'{data_edited_csv_filepath}', 'w', encoding="UTF-8", newline='') as file:
    file.write(header + "\n")
    #file.write('\n'.join(f'{tup[0][0]},{tup[0][1]},{tup[0][2]},{tup[0][3]},{tup[0][4]},{tup[0][5]},{tup[0][6]},{tup[1]}' for tup in data_first_fifty))
    file.write('\n'.join(f'{tup[0][0]},{tup[0][1]},{tup[0][2]},{tup[0][3]},{tup[0][4]},{tup[0][5]},{tup[0][6]},{tup[1]}' for tup in data))