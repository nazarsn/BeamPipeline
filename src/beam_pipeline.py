import datetime

import apache_beam as beam

FILE_PATH = 'gs://cloud-samples-data/bigquery/sample-transactions/transactions.csv'
OUTPUT = '../output/out_data'
OUTPUT_HEADERS = 'date,total_amount'
MINIMUM_YEAR = 2010
MINIMUM_TRANSACTIONS = 20.0
OUTPUT_DATE_FORMAT = '%Y-%m-%d'
TIMESTAMP_FORMAT = "%Y-%m-%d %H:%M:%S %Z"


class GetDateAndAmount(beam.DoFn):
    def process(self, item):
        date = get_datetime(item).strftime(OUTPUT_DATE_FORMAT)
        transaction_amount = item.transaction_amount
        yield date, transaction_amount


class FormatAsTextFn(beam.DoFn):
    def process(self, item):
        yield "%s,%s" % (item[0], item[1])


def get_datetime(item: beam.Row):
    return datetime.datetime.strptime(item.timestamp, TIMESTAMP_FORMAT)


@beam.ptransform_fn
def TransformCsv(pcoll):
    return (
            pcoll
            | beam.Map(lambda item: item.split(","))
            | beam.Map(lambda item: beam.Row(timestamp=item[0],
                                             origin=item[1],
                                             destination=item[2],
                                             transaction_amount=float(item[3])))
            | beam.Filter(lambda item: item.transaction_amount > MINIMUM_TRANSACTIONS)
            | beam.Filter(lambda item: get_datetime(item).year > MINIMUM_YEAR)
            | beam.ParDo(GetDateAndAmount())
            | beam.CombinePerKey(sum)
            | beam.ParDo(FormatAsTextFn()))


def main():
    pipeline = beam.Pipeline()

    task = (
            pipeline
            | beam.io.ReadFromText(FILE_PATH, skip_header_lines=True)
            | TransformCsv()
            | beam.io.WriteToText(OUTPUT, file_name_suffix='.csv', header=OUTPUT_HEADERS)
    )

    pipeline.run()


if __name__ == "__main__":
    main()
