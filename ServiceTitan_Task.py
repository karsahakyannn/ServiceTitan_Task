import pandas as pd
import pickle
from datetime import datetime

class DataExtractor:
    def __init__(self, invoice_file, expired_file):
        self.invoice_file = invoice_file
        self.expired_file = expired_file
        self.expired_invoices = self.load_expired_invoices()
        self.invoices = self.load_invoices()

    def load_expired_invoices(self):
        try:
            with open(self.expired_file, 'r') as file:
                expired_ids = file.read().strip().split(', ')
                return set(map(int, expired_ids))
        except Exception as e:
            print(f"Error loading expired invoices: {e}")
            return set()

    def load_invoices(self):
        try:
            with open(self.invoice_file, 'rb') as file:
                return pickle.load(file)
        except Exception as e:
            print(f"Error loading invoices: {e}")
            return []

    def convert_to_float(self, value):
        try:
            return float(value)
        except (ValueError, TypeError):
            return float('nan')

    def convert_to_datetime(self, date_str):
        try:
            return pd.to_datetime(date_str, errors='coerce')
        except Exception:
            return pd.NaT

    def transform_data(self):
        data = []
        type_conversion = {
            0: 'Material', 
            1: 'Equipment', 
            2: 'Service', 
            3: 'Other',
            'O': 'Other'  # Handling unexpected 'O' value
        }

        for invoice in self.invoices:
            if 'items' not in invoice:
                print(f"Skipping invoice ID {invoice['id']} due to missing 'items' key.")
                continue

            invoice_id = invoice['id']
            # Remove any non-numeric characters from invoice_id
            invoice_id = ''.join(filter(str.isdigit, str(invoice_id)))
            try:
                invoice_id = int(invoice_id)
            except ValueError:
                print(f"Invalid invoice ID {invoice['id']} after cleaning.")
                continue

            created_on = self.convert_to_datetime(invoice['created_on'])
            if pd.isna(created_on):
                print(f"Invalid date format for invoice ID {invoice_id}.")
                created_on = pd.NaT  # Set to Not a Time (NaT) to handle later

            total_invoice_price = sum(
                self.convert_to_float(item['item']['unit_price']) * self.convert_to_float(item['quantity'])
                for item in invoice['items']
            )
            is_expired = invoice_id in self.expired_invoices

            for item in invoice['items']:
                try:
                    invoiceitem_id = item['item']['id']
                    invoiceitem_id = ''.join(filter(str.isdigit, str(invoiceitem_id)))
                    invoiceitem_id = int(invoiceitem_id)
                    invoiceitem_name = item['item']['name']
                    type_ = type_conversion.get(item['item']['type'], 'Other')
                    unit_price = self.convert_to_float(item['item']['unit_price'])
                    quantity = self.convert_to_float(item['quantity'])
                    total_price = unit_price * quantity if not pd.isna(unit_price) and not pd.isna(quantity) else float('nan')
                    percentage_in_invoice = total_price / total_invoice_price if not pd.isna(total_price) and total_invoice_price != 0 else float('nan')

                    data.append({
                        'invoice_id': invoice_id,
                        'created_on': created_on,
                        'invoiceitem_id': invoiceitem_id,
                        'invoiceitem_name': invoiceitem_name,
                        'type': type_,
                        'unit_price': unit_price,
                        'total_price': total_price,
                        'percentage_in_invoice': percentage_in_invoice,
                        'is_expired': is_expired
                    })
                except KeyError as e:
                    print(f"Missing expected key in item for invoice ID {invoice_id}: {e}")
                except ValueError as e:
                    print(f"Error processing item for invoice ID {invoice_id}: {e}")

        df = pd.DataFrame(data)
        try:
            df = df.astype({
                'invoice_id': 'int',
                'created_on': 'datetime64[ns]',
                'invoiceitem_id': 'int',
                'invoiceitem_name': 'str',
                'type': 'str',
                'unit_price': 'float',
                'total_price': 'float',
                'percentage_in_invoice': 'float',
                'is_expired': 'bool'
            })
        except ValueError as e:
            print(f"Error converting DataFrame types: {e}")
        df = df.sort_values(by=['invoice_id', 'invoiceitem_id'])
        return df

    def save_to_csv(self, df, filename):
        try:
            df.to_csv(filename, index=False)
            print(f"Data saved to {filename} successfully.")
        except Exception as e:
            print(f"Error saving data to CSV: {e}")

# Usage
invoice_file = '/Users/karensahakyan/Desktop/ServiceTitan_Task/invoices_new.pkl'
expired_file = '/Users/karensahakyan/Desktop/ServiceTitan_Task/expired_invoices.txt'

extractor = DataExtractor(invoice_file, expired_file)
df = extractor.transform_data()
extractor.save_to_csv(df, 'transformed_invoices.csv')
