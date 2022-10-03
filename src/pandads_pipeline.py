import pandas


def main():
    # 'gs://cloud-samples-data/bigquery/sample-transactions/transactions.csv'

    df = pandas.read_csv("../data/transactions.csv")
    print(df)

    df = df.loc[df['transaction_amount'] > 20]
    print(df)

    df = df.loc[pandas.DatetimeIndex(df['timestamp']).year > 2010]
    print(df)

    df['total_amount'] = df.groupby('timestamp')['transaction_amount'].transform('sum')
    df['date'] = pandas.to_datetime(df['timestamp']).dt.strftime('%Y-%m-%d')

    result = df[['date', 'total_amount']]
    print(result)

    filename = "../output/result.json.gz"
    result.to_json(filename, orient='records', compression='gzip')


if __name__ == "__main__":
    main()
