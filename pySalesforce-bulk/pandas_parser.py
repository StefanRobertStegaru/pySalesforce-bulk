import pandas
import io


class PandasParser:
    def __init__(self):
        self.csv_separator = ','
        self.csv_compression = {"method": "gzip"}

    def get_pd_from_csv(self, csv):
        pandas_df = pandas.read_csv(io.StringIO(csv), sep=self.csv_separator)

        return pandas_df

    def get_csv_from_pd(self, pandas_df):

        # Prepare CSV based on the expected format from SF Bulk API v2.0
        csv = pandas_df.to_csv(sep=self.csv_separator, index=False,
                               compression=self.csv_compression)

        return csv
