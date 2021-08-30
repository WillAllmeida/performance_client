import datetime
import os
import helper as h
from measure import Measure
from sys import getsizeof
from termcolor import colored
from pyfiglet import Figlet
import click


@click.command()
@click.option('--rawrequests',
              'raw_requests',
              default="/../results/requests.parquet",
              help='Path to input with raw requests.')
@click.option('--requestsfile',
              'requests_file',
              default="/../results/sentrequests.parquet",
              help='Path to input with sent requests data.')
@click.option('--responsesfile',
              'responses_file',
              default="/../results/responses.parquet",
              help='Path to input with responses data.')
def main(raw_requests, requests_file, responses_file):
    # Initial Configuration
    result_df = h.load_input_files(raw_requests, requests_file, responses_file)
    color = 'cyan'
    measure = Measure(result_df)
    f = Figlet(font='slant')
    print(f.renderText('Performance Test'))

    bytes_received = measure.sum_bytes_count('Raw Response')
    bytes_sended = measure.sum_bytes_count('Serialized Request')
    test_duration = measure.calculate_test_duration()

    total_sending_time = measure.calculate_sending_time()
    total_receiving_time = measure.calculate_receiving_time()

    result_df['Latencies'] = measure.calculate_latency()

    throughput = measure.calculate_throughput()
    rate = measure.calculate_rate()

    success_rate = measure.calculate_success_rate()

    print("""Loaded files: \r\n"""
          f"""\t»» Prepared Requests: {colored(os.getcwd() + raw_requests, 'green')}\r\n"""
          f"""\t»» Sent Requests: {colored(os.getcwd() + requests_file, 'green')}\r\n"""
          f"""\t»» Responses: {colored(os.getcwd() + responses_file, 'green')}\r\n"""
          """\n"""
          )

    print('{} {}s'.format(h.format_result_index('Elapsed sending time'),
                          colored(total_sending_time, color)
                          ))
    print('{} {}s'.format(h.format_result_index('Elapsed receiving time'),
                          colored(total_receiving_time, color)
                          ))
    print('{} {}s'.format(h.format_result_index('Test duration'),
                          colored(test_duration, color)
                          ))
    print('{} {} MB'.format(h.format_result_index('Bytes sended'),
                            colored(round(bytes_sended, 3), color)
                            ))
    print('{} {} MB'.format(h.format_result_index('Bytes received'),
                            colored(round(bytes_received, 3), color)
                            ))
    print("""{} """
          """{}s, {}s, {}s"""
          .format(h.format_result_index('Latencies [min, max, mean]'),
                  colored(result_df['Latencies'].min(), color),
                  colored(result_df['Latencies'].max(), color),
                  colored(round(result_df['Latencies'].mean(), 3), color)
                  ))
    print("""{} """
          """{}, {}R/s, {} MB/s"""
          .format(h.format_result_index('Requests [count, rate, throughput]'),
                  colored(measure.requests_count, color),
                  colored(round(rate, 3), color),
                  colored(round(throughput, 3), color)
                  ))
    print('{} {}% '.format(h.format_result_index('Success rate'),
                           colored(success_rate, color)
                           ))


if __name__ == "__main__":
    main()
