from countries import countries_invalid, countries_all
import csv
import random

data_origin_csv_filepath = '/home/dusan/PycharmProjects/JDP_Data_Engineering_Task/venv/csvs/data.csv'
data_edited_csv_filepath = '/home/dusan/PycharmProjects/JDP_Data_Engineering_Task/venv/csvs/data_edited1.csv '
data_origin_dict = {
    "InvoiceNo": [],
    "StockCode": [],
    "Description": [],
    "Quantity": [],
    "InvoiceDate": [],
    "UnitPrice": [],
    "CustomerID": [],
    "Country": []
}

with open(f'{data_origin_csv_filepath}', 'r', encoding="ISO-8859-1") as csv_file:
    csv_reader = csv.DictReader(csv_file, delimiter=',')
    # next(csv_reader)  # to skip header
    count = 0

    for line in csv_reader:
        # count += 1
        # if count == 10:
        #    break
        # else:
        # print(line)
        # print('line length:', len(line))
        data_origin_dict["InvoiceNo"].append(line["InvoiceNo"])
        data_origin_dict["StockCode"].append(line["StockCode"])
        data_origin_dict["Description"].append(line["Description"])
        data_origin_dict["Quantity"].append(line["Quantity"])
        data_origin_dict["InvoiceDate"].append(line["InvoiceDate"])
        data_origin_dict["UnitPrice"].append(line["UnitPrice"])
        data_origin_dict["CustomerID"].append(line["CustomerID"])
        data_origin_dict["Country"].append(line["Country"])

print('data_origin_dict["InvoiceNo"] length:', len(data_origin_dict["InvoiceNo"]))
print('data_origin_dict["StockCode"] length:', len(data_origin_dict["StockCode"]))
print('data_origin_dict["Description"] length:', len(data_origin_dict["Description"]))
print('data_origin_dict["Quantity"] length:', len(data_origin_dict["Quantity"]))
print('data_origin_dict["InvoiceDate"] length:', len(data_origin_dict["InvoiceDate"]))
print('data_origin_dict["UnitPrice"] length:', len(data_origin_dict["UnitPrice"]))
print('data_origin_dict["CustomerID"] length:', len(data_origin_dict["CustomerID"]))
print('data_origin_dict["Country"] length:', len(data_origin_dict["Country"]))

### InvoiceNo #######################################
# print(data_origin_dict["InvoiceNo"])

'''
invoiceNo_not_six_chars_length = []
for invoiceNo in data_origin_dict["InvoiceNo"]:
    if invoiceNo.startswith('C'):
        invoiceNo_not_six_chars_length.append(invoiceNo)

print('invoiceNo_not_six_chars_length items:')
for item in invoiceNo_not_six_chars_length:
    print(item)
print('invoiceNo_not_six_chars_length items length:', len(invoiceNo_not_six_chars_length))
'''

### StockCode ###################################################################


### Description #################################################################


### Quantity ####################################################################


### InvoiceDate #################################################################


### UnitPrice ###################################################################


### CustomerID ##################################################################
'''
print(data_origin_dict["CustomerID"])
print('data_origin_dict["CustomerID"] len:', len(data_origin_dict["CustomerID"]))
customerID_not_five_chars_length = []
for customerID in data_origin_dict["CustomerID"]:
    if customerID == '':
        customerID_not_five_chars_length.append(customerID)
print('customerID_not_five_chars_length len:', len(customerID_not_five_chars_length))
'''

'''
indexes_where_customerID_is_null = []
# This way, we get ALL indexes of occurrences!!!!
indexes_where_customerID_is_null = [i for i, x in enumerate(data_origin_dict["CustomerID"]) if x == '']
invoiceNos_where_customerID_is_null = []
for customerID_null_index in indexes_where_customerID_is_null:
    invoiceNos_where_customerID_is_null.append(data_origin_dict["InvoiceNo"][customerID_null_index])
print('invoiceNos_where_customerID_is_null:')
for invoiceNo in invoiceNos_where_customerID_is_null:
    print(invoiceNo)
print('invoiceNos_where_customerID_is_null len:', len(invoiceNos_where_customerID_is_null))
print('invoiceNos_where_customerID_is_null DISTINCT len:', len(set(invoiceNos_where_customerID_is_null)))

print('indexes_where_customerID_is_null len:', len(indexes_where_customerID_is_null))

newInvoiceNos = []
newCustomerID = 100000

print("At the start, newInvoiceNos len is:", len(newInvoiceNos))
for index in indexes_where_customerID_is_null:
    invoiceNo = data_origin_dict["InvoiceNo"][index]
    if invoiceNo not in newInvoiceNos:
        newCustomerID += 1
        data_origin_dict["CustomerID"][index] = str(newCustomerID)
        newInvoiceNos.append(invoiceNo)
    else:
        data_origin_dict["CustomerID"][index] = str(newCustomerID)
print("At the end , newInvoiceNos len is:", len(newInvoiceNos))
print('Now: ')
customerID_not_five_chars_length = []
for customerID in data_origin_dict["CustomerID"]:
    if customerID == '':
        customerID_not_five_chars_length.append(customerID)
print('customerID_not_five_chars_length len:', len(customerID_not_five_chars_length))
print("Now:")
for customerID in data_origin_dict["CustomerID"]:
    print(customerID)
'''

### Country #####################################################################

print('Country values:')
print(data_origin_dict["Country"])
print('data_origin_dict["Country"] len:', len(data_origin_dict["Country"]))

countries_list = []
for country in data_origin_dict["Country"]:
    countries_list.append(country)

countries_set = set(countries_list)
print('Countries in countries_set:')
for country in countries_set:
    print(country)
print('countries_set len:', len(countries_set))

one_to_six_list = list(range(1, 7))
newInvoiceNos = []
rand_num = random.choices(one_to_six_list, k=6)[0]

for country in data_origin_dict["Country"]:
    if country in countries_invalid:
        idx = data_origin_dict["Country"].index(country)
        data_origin_dict["Country"][idx] = 'invalid'
        print('idx:', idx)
    else:
        idx = data_origin_dict["Country"].index(country)
        invoiceNo = data_origin_dict["InvoiceNo"][idx]
        countryId = countries_all[country]
        if invoiceNo not in newInvoiceNos:
            rand_num = random.choices(one_to_six_list, k=6)[0]
            data_origin_dict["Country"][idx] = str(countryId) + '-' + str(rand_num)
            newInvoiceNos.append(invoiceNo)
            print('idx:', idx)
        else:
            data_origin_dict["Country"][idx] = str(countryId) + '-' + str(rand_num)
            print('idx:', idx)

print(data_origin_dict["Country"])

with open(f'{data_edited_csv_filepath}', 'w', encoding="UTF-8", newline='') as csv_file:
    # pass the csv file to csv.writer function.
    writer = csv.writer(csv_file)

    # pass the dictionary keys to writerow
    # function to frame the columns of the csv file
    writer.writerow(data_origin_dict.keys())

    # make use of writerows function to append
    # the remaining values to the corresponding
    # columns using zip function.
    writer.writerows(zip(*data_origin_dict.values()))


