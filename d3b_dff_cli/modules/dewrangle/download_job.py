"""Download job results from Dewrangle."""

from . import helper_functions as hf

def download_job(jobid, token=None):
    """
    Function to download results from Dewrangle
    Input: Dewrangle job id
    Output: object with job resuls
    """

    client = hf.create_gql_client(api_key=token)

    return hf.download_job_result(jobid, client=client, api_key=token)


def main(args):
    """Main function."""

    status, job_df = download_job(args.jobid)
    if status == "Complete":
        job_df.to_csv(args.outfile)
    else:
        print("Job incomplete, please check again later.")
