# Aurora DSQL Migration Samples

[![Discord chat](https://img.shields.io/discord/1435027294837276802.svg?logo=discord)](https://discord.com/invite/nEF6ksFWru)

This repository contains samples that demonstrate how to migrate data into [Amazon Aurora DSQL](https://aws.amazon.com/rds/aurora/dsql/) from a variety of source databases and environments.

To get started with Aurora DSQL, create clusters, and for more information, please refer to the [AWS Documentation](https://docs.aws.amazon.com/aurora-dsql/latest/userguide/getting-started.html).

## How this repository is organized

Each subdirectory is a self-contained migration sample with its own README, architecture diagram, and deployment assets (e.g. CloudFormation templates, ETL scripts, DDL). The samples show different migration strategies — snapshot export + ETL, change-data capture, application-tier dual-writes, and more — for different source databases.

| Source                                | Migration Approach                      | Sample                                                                             |
|:--------------------------------------|:----------------------------------------|:-----------------------------------------------------------------------------------|
| Amazon Aurora PostgreSQL snapshot     | Snapshot → S3 → AWS Glue PySpark → DSQL | [Amazon_Aurora_snapshot_to_DSQL](Amazon_Aurora_snapshot_to_DSQL) |

More samples will be added over time. If you have a migration scenario you'd like to see covered, [open an issue](../../issues) or contribute a sample via a pull request.

## Related repositories

- [aws-samples/aurora-dsql-samples](https://github.com/aws-samples/aurora-dsql-samples) — connectors, ORM examples, authentication, and end-to-end sample applications for Aurora DSQL.

## Security

See [CONTRIBUTING](CONTRIBUTING.md#security-issue-notifications) for more information.

## License

This project is licensed under the MIT-0 License.
