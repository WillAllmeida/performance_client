import pandas as pd

class Measure:

    def __init__(self, df):
        self.df = df
        self.requests_count = len(df.index)

    def calculate_sending_time(self):
        start_time = self.df.at[0, 'Send Time']
        finish_time = self.df.at[self.requests_count - 1, 'Send Time']

        start_time_dt = pd.to_datetime(start_time, unit='ms')
        finish_time_dt = pd.to_datetime(finish_time, unit='ms')

        return (finish_time_dt - start_time_dt).total_seconds()

    def calculate_receiving_time(self):
        start_time_response = self.df.at[0, 'Receive Time']
        finish_time_response = self.df.at[self.requests_count - 1, 'Receive Time']

        start_time_dt = pd.to_datetime(start_time_response, unit='ms')
        finish_time_dt = pd.to_datetime(finish_time_response, unit='ms')

        return (finish_time_dt - start_time_dt).total_seconds()

    def calculate_test_duration(self):
        start_time = self.df.at[0, 'Send Time']
        finish_time_response = self.df.at[self.requests_count - 1, 'Receive Time']

        start_time_dt = pd.to_datetime(start_time, unit='ms')
        finish_time_dt = pd.to_datetime(finish_time_response, unit='ms')

        return (finish_time_dt - start_time_dt).total_seconds()

    def sum_bytes_sended(self):
        return self.df['Serialized Request'].apply(lambda x: len(x)).sum()

    def sum_bytes_received(self):
        return self.df['Raw Response'].apply(lambda x: len(x)).sum()

    def calculate_latency(self, row):
        send_time = pd.to_datetime(row['Send Time'], unit='ms')
        receive_time = pd.to_datetime(row['Receive Time'], unit='ms')
        return (receive_time - send_time).total_seconds()

    def calculate_throughput(self):
        throughput = self.sum_bytes_sended() / self.calculate_test_duration()

        return throughput

    def calculate_rate(self):
        rate = self.requests_count / self.calculate_test_duration()

        return rate

    def calculate_success_rate(self):
        status_code_count = self.df['Raw Response'].apply(lambda x: x[9:12]).value_counts()

        return (status_code_count['200'] / self.requests_count) * 100