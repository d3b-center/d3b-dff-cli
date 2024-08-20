import argparse
import sys
from .version import __version__
from .modules.validation.check_manifest import main as check_manifest
from .modules.validation.check_readgroup import main as check_readgroup
from .modules.validation.check_url import main as check_url
from .modules.dewrangle.volume import main as hash_volume
from .modules.dewrangle.list_jobs import main as list_jobs
from .modules.dewrangle.download_job import main as download_dewrangle_job
from .modules.intake.request import main as intake_request


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


def create_parser():
    """
    Create the main parser for the d3b-dff-cli command-line interface.
    """
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

    # Validation Command
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
    hash_parser = dewrangle_subparsers.add_parser(
        "hash", help="Hash volume in Dewrangle"
    )
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

    # Intake commands
    # request: create data transfer intake epic
    intake_parser = subparsers.add_parser("intake", help="Intake commands")
    intake_subparsers = intake_parser.add_subparsers(
        title="Intake Subcommands", dest="intake_command"
    )

    # request subcommand
    request_parser = intake_subparsers.add_parser(
        "request", help="Create data transfer to epic in Jira"
    )
    ## add intake request arguments
    request_parser.add_argument(
        "-auth",
        help="Base64 encoded Jira username and password",
        required=True,
    )
    request_parser.add_argument(
        "-jira_url",
        help="Jira url",
        required=True,
    )
    # need study, data_source, program, summary, prd, post
    request_parser.add_argument("-project", help="Jira project name", required=True)
    request_parser.add_argument("-issue_type", help="Jira issue_type", required=True)
    request_parser.add_argument("-fields", help="JSON-like dictionary of issue fields", required=False) # need to set this to True eventually
    request_parser.add_argument("-study", help="KF study name", required=False) # need to delete these eventually
    request_parser.add_argument("-data_source", help="Data source name", required=False)
    request_parser.add_argument("-program", help="Program name", required=False)
    request_parser.add_argument(
        "-summary", help="Optional summary for epic, overrides default", required=False
    )
    request_parser.add_argument(
        "-prd",
        help="Optional, remove TEST from summary, default false",
        required=False,
        default=False,
        action="store_true",
    )
    request_parser.add_argument(
        "-post",
        help="Optional, actually post request and make epic, default: dump json payload",
        required=False,
        default=False,
        action="store_true",
    )
    request_parser.set_defaults(func=intake_request)

    return parser


def main():
    """
    Main function, create argument parser, provide help messages, and call appropriate function.
    """
    # create parser
    parser = create_parser()

    args = parser.parse_args()

    # if no arguments given, print help message
    if len(sys.argv) == 1:
        parser.print_help(sys.stderr)
        sys.exit(1)

    # if function exists, call function. else fail and print error message
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
