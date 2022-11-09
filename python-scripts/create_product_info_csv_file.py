"""
The product info table will have 4 columns, StockCode, Product Description, Unit Price, and Date, the
data will be updated once a day from HDFS. It is important to mention that the same product can be
priced differently on different dates.
"""

import csv

#data_edited_csv_filepath = '/home/dusan/PycharmProjects/JDP_Data_Engineering_Task/venv/csvs/data_edited2.csv'
data_edited_csv_filepath = 'file:///home/scala/src/main/resources/csvs/data_edited.csv'
#product_info_csv_filepath = '/home/dusan/PycharmProjects/JDP_Data_Engineering_Task/venv/csvs/product_info2.csv'
product_info_csv_filepath = 'file:///home/scala/src/main/resources/csvs/product_info.csv'

data_product_info_dict = {
    "StockCode": [],
    "Description": [],
    "UnitPrice": [],
    "InvoiceDate": []
}

with open(f'{data_edited_csv_filepath}', 'r', encoding="ISO-8859-1") as csv_file:
    csv_reader = csv.DictReader(csv_file, delimiter=',')
    for line in csv_reader:
        data_product_info_dict["StockCode"].append(line["StockCode"])
        data_product_info_dict["Description"].append(line["Description"])
        data_product_info_dict["UnitPrice"].append(line["UnitPrice"])
        data_product_info_dict["InvoiceDate"].append(line["InvoiceDate"])

print('data_product_info_dict["StockCode"] length:', len(data_product_info_dict["StockCode"]))
print('data_product_info_dict["Description"] length:', len(data_product_info_dict["Description"]))
print('data_product_info_dict["UnitPrice"] length:', len(data_product_info_dict["UnitPrice"]))
print('data_product_info_dict["InvoiceDate"] length:', len(data_product_info_dict["InvoiceDate"]))

with open(f'{product_info_csv_filepath}', 'w', encoding="UTF-8", newline='') as csv_file:
    writer = csv.writer(csv_file)
    writer.writerow(data_product_info_dict.keys())
    writer.writerows(zip(*data_product_info_dict.values()))