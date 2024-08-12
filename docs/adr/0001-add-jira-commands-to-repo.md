# 1. Add Jira Commands to D3B DFF ClI Repo

## Status

Partially Implemented

## Context

This repository contains the DFF CLI that is being developed as a cli and as a set of Python modules that can be imported within other scripts. The [Data Transfer step function](https://github.com/d3b-center/d3b-dff-data-transfer-pipeline) imports the validation and Dewrangle functions within most steps of that pipeline. We want to update the data transfer step function to add a comment on a Jira ticket when most steps of the pipeline are finished and when errors occur in the pipeline.

The Data Transfer pipeline uses [notification lambas]() that send Slack messages, adds Jira comments, and sets the Jira status of tickets while the pipeline runs. But, those lambdas are only triggered when a new file is created in a manifest bucket or when the step function changes status ("RUNNING", "SUCCEDED", or "FAILED"). When a step function progresses from one step to another, the status is not changed so the status notification lambda can't be used to send alerts when a step finishes. The repo with the notification lambdas is private, so it can't be included in the Dockerfiles of the step function lambdas.

The notification lambdas are only Python scripts not Docker images and so can't have additional libraries installed.

## Decision

Copy the add_jira_comment and check_status functions into the D3B DFF CLI so the Data Transfer step function can create Jira comments recording step function progress. The functions are currently not added to the cli because there are no existing plans to use them as such.

## Consequences

The same or very simliar versions of some Jira functions exist and need to be maintained in two separate repos. One here and another copy in the notification lambdas repo.
