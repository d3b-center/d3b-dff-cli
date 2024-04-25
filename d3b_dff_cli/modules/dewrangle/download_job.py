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

    status, job_res = download_job(args.jobid)
    if status == "Complete":
        with open(args.outfile, "w") as f:
            for line in job_res:
                f.write("%s\n" % ",".join(line))
    else:
        print("Job incomplete, please check again later.")
