import pyarrow.parquet as pq
import os


def load_input_files(requests_file, sentrequests_file, responses_file):

    requests_df = pq.read_table(os.getcwd() + requests_file,
                                columns=['Message ID',
                                         'Serialized Request'
                                         ]
                                ).to_pandas()
    sentrequests_df = pq.read_table(os.getcwd() + sentrequests_file,
                                    columns=['Message ID', 'Send Time']
                                    ).to_pandas()
    responses_df = pq.read_table(os.getcwd() + responses_file,
                                 columns=['Message ID',
                                          'Receive Time',
                                          'Raw Response'
                                          ]
                                 ).to_pandas()

    return (requests_df.merge(sentrequests_df, on='Message ID'
                              )
                       .merge(responses_df, on='Message ID'))


def format_result_index(text):
    whitespaces_count = 45 - len(text)

    return(text + ' ' * whitespaces_count)
