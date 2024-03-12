import argparse
import sys
from .version import __version__
from .modules.validation.check_manifest import main as check_manifest
from .modules.validation.check_readgroup import main as check_readgroup
from .modules.validation.check_url import main as check_url
from .modules.dewrangle.volume import load_and_hash_volume


def add_hash_arguments(my_parser):
    """
    Create parser for volume hash subcommand.
    Input:
        - my_parser: argparse parser being added to
    Output:
        - original parser with added arguments
    """
    hash_parser = my_parser.add_parser(
        "hash", help="Hash volume in Dewrangle"
    )
    hash_parser.add_argument(
        "-prefix",
        help="Optional, Path prefix. Default: None",
        default=None,
        required=False,
    )
    hash_parser.add_argument(
        "-region",
        help="Optional, Bucket AWS region code. Default: us-east-1",
        default="us-east-1",
        required=False,
    )
    hash_parser.add_argument(
        "-billing",
        help="Optional, billing group name. When not provided, use default billing group for organization",
        default=None,
        required=False,
    )
    hash_parser.add_argument(
        "-credential",
        help="Dewrangle AWS credential name. Default, try to find available credential.",
        required=False,
    )
    hash_parser.add_argument(
        "-study", help="Study name, global id, or study id", required=True
    )
    hash_parser.add_argument("-bucket", help="Bucket name", required=True)
    hash_parser.set_defaults(func=load_and_hash_volume)

    return hash_parser



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

    # Volume Command
    volume_parser = subparsers.add_parser("volume", help="Dewrangle volume commands")
    volume_subparsers = volume_parser.add_subparsers(
        title="Dewrangle Subcommands", dest="volume_command"
    )

    # volume hash subcommand
    hash_parser = add_hash_arguments(volume_subparsers)

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
