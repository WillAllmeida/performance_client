import datetime
import helper
from measure import Measure
from sys import getsizeof


if __name__ == "__main__":
    result_df = helper.load_input_files()

    measure = Measure(result_df)

    bytes_received = measure.sum_bytes_received()
    bytes_sended = measure.sum_bytes_sended()
    test_duration = measure.calculate_test_duration()

    total_sending_time = measure.calculate_sending_time()
    total_receiving_time = measure.calculate_receiving_time()

    result_df['Latencies'] = result_df[['Send Time','Receive Time']].apply(lambda x: measure.calculate_latency(x), axis=1)
    
    throughput = measure.calculate_throughput()
    rate = measure.calculate_rate()

    success_rate = measure.calculate_success_rate()

    print('{}s of elapsed sending time'.format(total_sending_time))
    print('{}s of elapsed receiving time'.format(total_receiving_time))
    print('{}s of test duration'.format(test_duration))
    print('{} Bytes sended'.format(bytes_sended))
    print('{} Bytes received'.format(bytes_received))
    print('Latencies [min, max, mean]   {}s, {}s, {}s'.format(result_df['Latencies'].min(), result_df['Latencies'].max(), result_df['Latencies'].mean()))
    print('Requests [count, rate(Requests/s), throughput(Bytes/s)]   {}, {}, {}'.format(len(result_df.index), rate, throughput))
    print('{}% Success rate'.format(success_rate))