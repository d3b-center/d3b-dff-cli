import argparse
from .version import __version__
from .modules.validation.check_manifest import main as check_manifest
from .modules.validation.check_readgroup import main as check_readgroup
from .modules.validation.check_url import main as check_url
from .modules.dewrangle.volume import dewrangle_volume

def main():
    parser = argparse.ArgumentParser(description='A command-line interface for d3b-dff-cli.')
    subparsers = parser.add_subparsers(title='Available Commands', dest='command')

    # Version
    version_parser = subparsers.add_parser("version", help="Display version information.")
    version_parser.set_defaults(func=lambda args: print(f"d3b-dff-cli version {__version__}"))

    # Validation Command
    validation_parser = subparsers.add_parser('validation', help='Validation commands')
    validation_subparsers = validation_parser.add_subparsers(title='Validation Subcommands', dest='validation_command')

    ## validation manifest subcommand
    manifest_parser = validation_subparsers.add_parser('manifest', help='Manifest validation based on defined rules.')
    manifest_parser.add_argument('-rules', help='Formatted JSON file defining validation rules.')
    manifest_parser.add_argument('-rule_type', help='Specific type of validation rule defined in the json rule file.')
    manifest_parser.add_argument('-manifest_file', help='Manifest based on the d3b genomics manifest template.')
    manifest_parser.set_defaults(func=check_manifest)

    ## validation read-group subcommand
    parser_bam = validation_subparsers.add_parser("bam", help="Validator for BAM file @RG based on Samtools.")
    parser_bam.add_argument("bam_files", nargs="+", help="One or more BAM files to validate #RG")
    parser_bam.set_defaults(func=check_readgroup)

    ## validation url subcommand
    parser_url = validation_subparsers.add_parser("url", help="Validator for URLs")
    parser_url.add_argument("urls", nargs="+", help="One or more URLs to validate")
    parser_url.set_defaults(func=check_url)

    # Dewrangle Command
    dewrangle_parser = subparsers.add_parser('dewrangle', help='Dewrangle commands')
    dewrangle_subparsers = dewrangle_parser.add_subparsers(title='Dewrangle Subcommands', dest='dewrangle_command')
    # dewrangle volume subcommand
    dewrangle_volume_parser = dewrangle_subparsers.add_parser("volume", help="Dewrangle volume")
    dewrangle_volume_parser.set_defaults(func=lambda args: dewrangle_volume())

    args = parser.parse_args()
    if hasattr(args, "func"):
        args.func(args)
    else:
        print("Invalid command. Use --help for usage information.")

if __name__ == "__main__":
    main()
