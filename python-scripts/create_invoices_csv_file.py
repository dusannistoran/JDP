import csv
import configparser

config = configparser.ConfigParser()
config.read('config.ini')

data_edited_csv_filepath = config['data_edited']['path']
invoices_csv_filepath = config['invoices']['path']

with open(data_edited_csv_filepath, 'r', encoding="ISO-8859-1") as f:
    reader = csv.DictReader(f)

    column_names = next(reader) # Reads the first line, which contains the header
    data_invoice = {col: [] for col in column_names}
    for row in reader:
        for key, value in zip(column_names, row):
            data_invoice[key].append(value)

with open(invoices_csv_filepath, 'w', encoding="UTF-8", newline='') as csv_file:
    writer = csv.writer(csv_file)
    writer.writerow(data_invoice.keys())
    writer.writerows(zip(*data_invoice.values()))
