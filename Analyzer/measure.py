import pandas as pd

class Measure:

    def __init__(self, df):
        self.df = df
        self.requests_count = len(df.index)

    def calculate_sending_time(self):
        start_time = self.df.at[0, 'Send Time']
        finish_time = self.df.at[self.requests_count - 1, 'Send Time']

        return (int(finish_time) - int(start_time)) / 1000

    def calculate_receiving_time(self):
        start_time_response = self.df.at[0, 'Receive Time']
        finish_time_response = self.df.at[self.requests_count - 1, 'Receive Time']

        return (int(finish_time_response) - int(start_time_response)) / 1000

    def calculate_test_duration(self):
        start_time = self.df.at[0, 'Send Time']
        finish_time_response = self.df.at[self.requests_count - 1, 'Receive Time']

        return (int(finish_time_response) - int(start_time)) / 1000

    def sum_bytes_count(self, column):
        return self.df[column].apply(lambda x: len(x)).sum()

    def calculate_latency(self):
        return (self.df['Receive Time'].astype(int) - self.df['Send Time'].astype(int)) / 1000

    def calculate_throughput(self):
        throughput = self.sum_bytes_count('Serialized Request') / self.calculate_test_duration()

        return throughput

    def calculate_rate(self):
        rate = self.requests_count / self.calculate_test_duration()

        return rate

    def calculate_success_rate(self):
        status_code_count = self.df['Raw Response'].apply(lambda x: x[9:12]).value_counts()

        return (status_code_count['200'] / self.requests_count) * 100