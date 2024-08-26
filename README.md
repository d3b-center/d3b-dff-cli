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

### Dewrangle
To perform dewrangling tasks, use the dewrangle command with subcommands:
```bash
d3b dewrangle
Subparser 'dewrangle'
usage: d3b dewrangle [-h] {hash,list_jobs,download} ...

optional arguments:
  -h, --help            show this help message and exit

Dewrangle Subcommands:
  {hash,list_jobs,download}
    hash                Hash volume in Dewrangle
    list_jobs           List volume jobs in Dewrangle
    download            Download job results from Dewrangle
```
Note: In Dewrangle, volumes are AWS s3 buckets with or without a prefix (sub-directory). Studies are collections of volumes. Generally, we prefer to have studies correspond to AWS accounts. It is also preferable to add and hash an entire bucket to avoid the costs associated with launching multiple hash jobs should you need them later.

If a volume was previously hashed and a new hash job is launched, only new or modified files will be hashed in the new job. All files in the bucket will be included in the results file.

#### Dewrangle Personal Access Token
To access Dewrangle, you must sign in to [Dewrangle](dewrangle.com) and generate a personal access token. Once signed in, click on your profile icon, click "Settings", then click "Generate new token". Name your token and copy the token string. Paste the token into a file called `~/.dewrangle/credentials`.

###### Dewrangle Credentials File
```bash
[default]
  api_key = "<your token string>"
```

### Jira
Create Jira ticket / epic
```bash
d3b jira
Subparser 'jira'
usage: d3b jira [-h] {create_ticket} ...

optional arguments:
  -h, --help       show this help message and exit

Jira Subcommands:
  {create_ticket}
    create_ticket  Create data transfer to epic in Jira
```
