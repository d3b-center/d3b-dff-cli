import argparse
import sys
from .version import __version__
from .modules.validation.check_manifest import main as check_manifest
from .modules.validation.check_readgroup import main as check_readgroup
from .modules.validation.check_url import main as check_url
from .modules.dewrangle.volume import main as hash_volume
from .modules.dewrangle.list_jobs import main as list_jobs
from .modules.dewrangle.download_job import main as download_dewrangle_job
from .modules.registration.create_ingest_package import main as create_ingest_package
from .modules.registration.run_ingest_package import main as run_ingest_package
from .modules.registration.registration_qc import main as registration_qc

def add_dewrangle_arguments(my_parser):
    """
    Add standard arguments for Dewrangle subcommands.
    Input:
        - my_parser: argparse parser being added to
    Output:
        - original parser with added arguments
    """
    my_parser.add_argument(
        "-prefix",
        help="Optional, Path prefix. Default: None",
        default=None,
        required=False,
    )
    my_parser.add_argument(
        "-region",
        help="Optional, Bucket AWS region code. Default: us-east-1",
        default="us-east-1",
        required=False,
    )
    my_parser.add_argument(
        "-billing",
        help="Optional, billing group name. When not provided, use default billing group for organization",
        default=None,
        required=False,
    )
    my_parser.add_argument(
        "-credential",
        help="Dewrangle AWS credential name. Default, try to find available credential.",
        required=False,
    )
    my_parser.add_argument(
        "-study", help="Study name, global id, or study id", required=True
    )
    my_parser.add_argument("-bucket", help="Bucket name", required=True)

    return my_parser


def main():
    parser = argparse.ArgumentParser(
        description="A command-line interface for d3b-dff-cli."
    )
    subparsers = parser.add_subparsers(title="Available Commands", dest="command")

    # Version
    version_parser = subparsers.add_parser(
        "version", help="Display version information."
    )
    version_parser.set_defaults(
        func=lambda args: print(f"d3b-dff-cli version {__version__}")
    )

    # Validation Commands
    validation_parser = subparsers.add_parser("validation", help="Validation commands")
    validation_subparsers = validation_parser.add_subparsers(
        title="Validation Subcommands", dest="validation_command"
    )

    ## validation manifest subcommand
    manifest_parser = validation_subparsers.add_parser(
        "manifest", help="Manifest validation based on defined rules."
    )
    manifest_parser.add_argument(
        "-rules", help="Formatted JSON file defining validation rules.", required=True
    )
    manifest_parser.add_argument(
        "-rule_type",
        help="Specific type of validation rule defined in the json rule file.",
        required=True,
    )
    manifest_parser.add_argument(
        "-manifest_file",
        help="Manifest based on the d3b genomics manifest template.",
        required=True,
    )
    manifest_parser.set_defaults(func=check_manifest)

    ## validation read-group subcommand
    parser_bam = validation_subparsers.add_parser(
        "bam", help="Validator for BAM file @RG based on Samtools."
    )
    parser_bam.add_argument(
        "bam_files", nargs="+", help="One or more BAM files to validate #RG"
    )
    parser_bam.set_defaults(func=check_readgroup)

    ## validation url subcommand
    parser_url = validation_subparsers.add_parser("url", help="Validator for URLs")
    parser_url.add_argument("urls", nargs="+", help="One or more URLs to validate")
    parser_url.set_defaults(func=check_url)

    # Dewrangle commands
    # hash: load a bucket to Dewrangle and hash it
    # list_jobs: list jobs run on a bucket
    # download: download the results of a job
    dewrangle_parser = subparsers.add_parser("dewrangle", help="Dewrangle commands")
    dewrangle_subparsers = dewrangle_parser.add_subparsers(
        title="Dewrangle Subcommands", dest="dewrangle_command"
    )

    # hash subcommand
    hash_parser = dewrangle_subparsers.add_parser("hash", help="Hash volume in Dewrangle")
    hash_parser = add_dewrangle_arguments(hash_parser)
    hash_parser.set_defaults(func=hash_volume)

    # list_jobs subcommand
    list_parser = dewrangle_subparsers.add_parser(
        "list_jobs", help="List volume jobs in Dewrangle"
    )
    list_parser = add_dewrangle_arguments(list_parser)
    list_parser.set_defaults(func=list_jobs)

    # download subcommand
    dl_parser = dewrangle_subparsers.add_parser(
        "download", help="Download job results from Dewrangle"
    )
    dl_parser.add_argument(
        "-jobid",
        help="Dewrangle jobid",
        required=True,
    )
    dl_parser.add_argument(
        "-outfile",
        help="Output file name",
        required=True,
    )
    dl_parser.set_defaults(func=download_dewrangle_job)


    # Registration Commands
    registration_parser = subparsers.add_parser(
        "registration", 
        help="Registration commands",
        description="This command handles the registration process. Please check https://github.com/d3b-center/d3b-dff-cli/data/registration/README.md for details."
    )
    registration_subparsers = registration_parser.add_subparsers(
        title="Registration Subcommands", dest="registration_command"
    )

    # Create ingest package
    create_parser = registration_subparsers.add_parser(
        "create", help="Create a new ingest package."
    )
    create_parser.add_argument(
        "--input",
        help="Path to the JSON file for registration",
        required=True,
    )
    create_parser.set_defaults(func=create_ingest_package)

    # Run ingest package
    ingest_parser = registration_subparsers.add_parser(
        "run", help="Run the pipeline to ingest the package."
    )
    ingest_parser.add_argument("INGEST_PACKAGE_PATH", help="Path to the data ingest package directory.")
    ingest_parser.set_defaults(func=run_ingest_package)

    # Registration QC step
    qc_parser = registration_subparsers.add_parser(
        "check", help="Run QC step after registration."
    )
    qc_parser.add_argument(
        "-date",
        help="Registration date, YYYY-MM-DD (e.g., 2024-7-20).",
        required=True,
    )
    qc_parser.set_defaults(func=registration_qc)

    args = parser.parse_args()

    # if no arguments given, print help message
    if len(sys.argv) == 1:
        parser.print_help(sys.stderr)
        sys.exit(1)

    if hasattr(args, "func"):
        args.func(args)
    else:
        # something went wrong. Probably a command with no options. Print command's help
        # retrieve subparsers from parser
        subparsers_actions = [
            action
            for action in parser._actions
            if isinstance(action, argparse._SubParsersAction)
        ]
        # loop through subparser actions and find the command we tried to run
        for subparsers_action in subparsers_actions:
            # get all subparsers and print help
            for choice, subparser in subparsers_action.choices.items():
                if choice == args.command:
                    print("Subparser '{}'".format(choice))
                    print(subparser.format_help())
                    sys.exit(2)


if __name__ == "__main__":
    main()
