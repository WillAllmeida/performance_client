import pyarrow.parquet as pq


def load_input_files():
    requests_df = pq.read_table("../results/requests.parquet",
                                columns=['Message ID',
                                         'Serialized Request'
                                         ]
                                ).to_pandas()
    sentrequests_df = pq.read_table("../results/sentrequests.parquet",
                                    columns=['Message ID', 'Send Time']
                                    ).to_pandas()
    responses_df = pq.read_table("../results/responses.parquet",
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
