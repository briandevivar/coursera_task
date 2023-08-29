import os
import logging
import pandas as pd

from datetime import datetime

today = datetime.today()
date_str = today.strftime('%Y%m%d%H%M%S')


def log(message):
    logging.basicConfig(
        filename=os.path.join(os.path.dirname(__file__), 'logs', f'etl_{date_str}.log'),
        format='%(asctime)s - %(message)s',
        datefmt='%Y-%m-%dT%H:%M:%SZ',
        level=logging.DEBUG,
        encoding='utf-8'
    )
    logging.info(message)


def extract_from_json():
    file_to_process = os.path.join(os.path.dirname(__file__), 'data', 'bank_market_cap_1.json')
    df = pd.read_json(file_to_process, encoding='utf-8')
    return df


def extract_from_csv():
    file_to_process = os.path.join(os.path.dirname(__file__), 'data', 'exchange_rates.csv')
    df = pd.read_csv(file_to_process, encoding='utf-8', index_col=0)
    return df


def transform(data, exchange_rate):
    df = pd.DataFrame(data)
    df['Market Cap (US$ Billion)'] = df['Market Cap (US$ Billion)'].apply(lambda x: round(x * exchange_rate, 3))
    df.columns = df.columns.str.replace('US', 'GBP')
    return df


def load(data):
    csv_file = os.path.join(os.path.dirname(__file__), 'data', 'bank_market_cap_gbp.csv')
    data.to_csv(csv_file, index=False, encoding='utf-8')
    pass


def main():
    log(message='ETL Job Started')

    log(message='Extract Phase Started')

    exchange_rates = extract_from_csv()
    exchange_rate_to_gbp = exchange_rates.loc['GBP', 'Rates']

    print(f'EXCHANGE RATE US TO GBP: {exchange_rate_to_gbp}')

    bank_df = extract_from_json()

    print(bank_df.head(6))

    log(message='Extract Phase Ended')

    log(message='Transform Phase Started')
    transformed_data = transform(bank_df, exchange_rate_to_gbp)

    print(transformed_data.head(6))

    log(message='Transform Phase Ended')

    log(message='Load Phase Started')
    load(transformed_data)

    log(message='Load Phase Ended')


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    main()
