"""
Server logs will come on HDFS each hour. ...
The data will come as lines of text (RDDs). RDDs will have Invoice No, Stock code, Quantity, Invoice
date, CustomerID, and Country.
"""
import csv

data_edited_csv_filepath = '/home/dusan/PycharmProjects/JDP_Data_Engineering_Task/venv/csvs/data_edited2.csv'
logs_csv_filepath = '/home/dusan/PycharmProjects/JDP_Data_Engineering_Task/venv/csvs/logs2.csv'

data_logs_dict = {
    "InvoiceNo": [],
    "StockCode": [],
    "Quantity": [],
    "InvoiceDate": [],
    "CustomerID": [],
    "Country": []
}

with open(f'{data_edited_csv_filepath}', 'r', encoding="ISO-8859-1") as csv_file:
    csv_reader = csv.DictReader(csv_file, delimiter=',')
    for line in csv_reader:
        data_logs_dict["InvoiceNo"].append(line["InvoiceNo"])
        data_logs_dict["StockCode"].append(line["StockCode"])
        data_logs_dict["Quantity"].append(line["Quantity"])
        data_logs_dict["InvoiceDate"].append(line["InvoiceDate"])
        data_logs_dict["CustomerID"].append(line["CustomerID"])
        data_logs_dict["Country"].append(line["Country"])

print('data_logs_dict["InvoiceNo"] length:', len(data_logs_dict["InvoiceNo"]))
print('data_logs_dict["StockCode"] length:', len(data_logs_dict["StockCode"]))
print('data_logs_dict["Quantity"] length:', len(data_logs_dict["Quantity"]))
print('data_logs_dict["InvoiceDate"] length:', len(data_logs_dict["InvoiceDate"]))
print('data_logs_dict["CustomerID"] length:', len(data_logs_dict["CustomerID"]))
print('data_logs_dict["Country"] length:', len(data_logs_dict["Country"]))

with open(f'{logs_csv_filepath}', 'w', encoding="UTF-8", newline='') as csv_file:
    writer = csv.writer(csv_file)
    writer.writerow(data_logs_dict.keys())
    writer.writerows(zip(*data_logs_dict.values()))
