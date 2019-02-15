# BLAST+GCP Integration Project

## Building and Running
Please see the document [RUNNING_PIPELINE.md](https://github.com/ncbi/blast-gcp/blob/engineering/RUNNING_PIPELINE.md).

## Contributing
Please use the [gitflow branching model](https://www.atlassian.com/git/tutorials/comparing-workflows/gitflow-workflow):

* Do not check into the `master` branch.
* The `engineering` branch is the main development area, it should be kept stable (i.e.: builds, all tests pass) at all times.
* For each sprint, a branch is created from the `engineering` branch and is the starting point for other (feature) branches for individual development.
* Developers merge their changes into the sprint branch as they become stable.
* At the end of each sprint: the sprint branch is merged into engineering.

### Contact:
email: blast-gcp@ncbi.nlm.nih.gov

### Status:
This repository serves a project to run the blast algorithm within GCP.



BLAST+GCP Development Team
