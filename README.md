# d3b-dff-cli

## Description

This is a command-line interface (CLI) for the d3b-dff package.

## Installation

Clone the repository:

```bash
git clone git@github.com:d3b-center/d3b-dff-cli.git
cd d3b-dff-cli
```
Install the package using pip:

```bash
pip install .
```

## Package Usage
### Command Overview
```bash
d3b -h
usage: d3b [-h] {version,validation,dewrangle} ...

A command-line interface for d3b-dff-cli.

optional arguments:
  -h, --help            show this help message and exit

Available Commands:
  {version,validation,dewrangle}
    version             Display version information.
    validation          Validation commands
    dewrangle           Dewrangle commands
```

### Version
To display the version information:
```bash
d3b version
```

### Validation
To perform validation, use the validation command with subcommands:

```bash
d3b validation  -h
usage: d3b validation [-h] {manifest,bam,url} ...

optional arguments:
  -h, --help          show this help message and exit

Validation Subcommands:
  {manifest,bam,url}
    manifest          Manifest validation
    bam               Validator for BAM file @RG based on Samtools
    url               Validator for URLs
```

### Dewrangle(WIP)
To perform dewrangling tasks, use the dewrangle command with subcommands:
```bash
d3b dewrangle volume
```