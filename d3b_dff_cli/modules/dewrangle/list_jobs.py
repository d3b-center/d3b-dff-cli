"""List jobs run on a bucket using the Dewrangle GraphQL API."""

from . import helper_functions as hf

def list_volume_jobs(
    bucket_name,
    study_name,
    prefix=None,
    token=None,
):
    """
    Wrapper function that checks if a volume is loaded, and hashes it.
    Inputs: AWS bucket name, study name, aws region, and optional volume prefix.
    Output: job id of parent job created when volume is hashed.
    """

    client = hf.create_gql_client(api_key=token)

    # get study and org ids
    study_id = hf.get_study_id(client, study_name)

    # check if volume loaded to study
    study_volumes = hf.get_study_volumes(client, study_id)
    volume_id = hf.process_volumes(study_id, study_volumes, vname=bucket_name, prefix=prefix)

    print(volume_id)

    jobs = hf.get_volume_jobs(client, volume_id)

    # print all jobs
    print(
        "========================================================================================"
    )
    print("All jobs in volume:")
    print("JobID|createdAt|completedAt|Job_Type")
    for job in jobs:
        print(
            "{} | {} | {} | {}".format(
                job,
                jobs[job]["createdAt"],
                jobs[job]["completedAt"],
                jobs[job]["operation"],
            )
        )

    print(
        "========================================================================================"
    )

    # get most recent job and print id
    print(
        "Most recent hash job id: {}".format(
            hf.get_most_recent_job(client, volume_id, "hash")
        )
    )
    print(
        "Most recent list job id: {}".format(
            hf.get_most_recent_job(client, volume_id, "list")
        )
    )

    print("Done!")

    return


def main(args):
    """Main function."""

    list_volume_jobs(
        args.bucket,
        args.study,
        args.prefix,
    )
