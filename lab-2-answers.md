# Lab 2 - Object storage with S3

## Why are the credentials stored in a Secret and not in the ConfigMap?

A ConfigMap is made for regular configuration and anyone who can read it sees the values in plain text. The endpoint and the bucket name are not sensitive so it is fine to put them there. The access key, secret key and session token give access to the bucket, so they go in a Secret. Secrets can have their own access rules with RBAC, they are not shown by default with kubectl and they can be encrypted in etcd.

## The credentials of Onyxia are temporary. What happens if the Job is executed again tomorrow? How would a production platform provide credentials to a Job?

The session token saved in the Secret will have expired, so the aws command in the Job will fail with an ExpiredToken or AccessDenied error and the upload will not happen. We would need to recreate the Secret with new credentials before running it again.

In production we would not use the credentials of a user. The Job would run with its own service account that has only the permissions it needs, and it would get short lived credentials automatically, for example with workload identity like IRSA on AWS. Another option is a secret manager like Vault or External Secrets that rotates the credentials and updates the Secret.

## How would you turn this Job into a daily ingestion?

I would replace the Job by a CronJob. It uses the same pod template with a schedule added, for example `0 2 * * *` to run every day at 2am. In a real setup the data would also be read from the source system instead of a ConfigMap, and the date would be added to the object key, like `bronze/users/2026-10-10.csv`, so each run keeps its own file instead of overwriting the previous one.
