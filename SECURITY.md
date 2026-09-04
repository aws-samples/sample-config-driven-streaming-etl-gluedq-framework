# Security

## Reporting a Vulnerability

If you discover a potential security issue in this project, we ask that you notify AWS Security
via our [vulnerability reporting page](https://aws.amazon.com/security/vulnerability-reporting/).
Please do **not** create a public GitHub issue.

## Security Design

This framework deploys a CDC streaming ETL pipeline across multiple AWS services. The following
security controls are implemented:

### Authentication and Secrets
- Amazon RDS and Amazon MSK credentials stored in AWS Secrets Manager
- Secrets encrypted with a customer-managed AWS KMS key (rotation enabled)
- IAM database authentication enabled on Amazon RDS
- Amazon MSK uses SASL/SCRAM-SHA-512 with TLS enforcement

### Network Isolation
- All services deployed in private subnets within a dedicated VPC
- Amazon RDS is not publicly accessible
- Security groups use least-privilege rules with security group references (no broad CIDRs)
- VPC Flow Logs enabled for network monitoring

### Encryption
- S3 buckets encrypted with SSE-KMS (customer-managed key)
- Amazon RDS storage encryption with KMS
- Amazon MSK in-cluster and client-broker TLS encryption
- Amazon SNS topic encrypted with KMS

### Access Control
- Per-Lambda IAM roles with scoped permissions
- S3 bucket policies enforce TLS via `aws:SecureTransport` condition
- S3 Block Public Access enabled on all buckets
- No hardcoded credentials in code or templates

### Logging and Monitoring
- S3 access logging via dedicated logging bucket
- S3 versioning enabled on all buckets
- Amazon MSK broker logs to Amazon CloudWatch
- VPC Flow Logs to Amazon CloudWatch
- Lambda concurrency limits to prevent runaway execution

## Accepted Security Debt

The following items are documented as accepted trade-offs for this sample code:

| # | Item | Rationale |
|---|------|-----------|
| 1 | `Resource: '*'` on EC2 ENI Describe actions (Glue, Lambda roles) | AWS IAM does not support resource-level permissions for `ec2:DescribeNetworkInterfaces` and similar Describe actions |
| 2 | `Resource: '*'` on Kafka read-only actions (`kafka:ListClusters`) | ListClusters does not support resource-level permissions |
| 3 | `Resource: '*'` on `logs:CreateLogGroup` (bootstrap Lambda) | Custom resource log group name is unknown at deploy time |
| 4 | Amazon RDS without deletion protection | Acceptable for sample code; enable `DeletionProtection: true` for production |
| 5 | AWS Glue job missing security configuration (CKV_AWS_195) | Recommended for production; optional for sample code |
| 6 | Lambda environment variables not encrypted with KMS (CKV_AWS_173) | Environment variables contain ARNs and hostnames, not secrets |

## Security Hardening Applied

Items raised during security review and the hardening applied for each:

| # | Item | Status |
|---|------|--------|
| 1 | Compliance callouts for examples handling PII/PHI | **Implemented** — compliance notes added to both example configs and this file. `customer_name`/`customer_phone` in the deliveries table are now masked via `mask_pii`, consistent with the healthcare example. **Customers deploying with real data are responsible for their own HIPAA/GDPR safeguards, including a Business Associate Agreement (BAA) with AWS where required.** See [AWS HIPAA compliance](https://aws.amazon.com/compliance/hipaa-compliance/). |
| 2 | Utility Lambdas have no DLQ | **Implemented** — a shared SQS DLQ (`{stack}-lambda-dlq`, 14-day retention) is attached to all five utility Lambdas. They are invoked synchronously today (DLQs apply to async invocations), so this is forward-looking failure capture. The queue is encrypted with the stack's customer-managed KMS key (`KmsMasterKeyId`), satisfying Checkov `CKV_AWS_27`; senders hold the matching `kms:Decrypt` / `kms:GenerateDataKey` grant. |
| 3 | KMS key admin statement used wildcard sub-actions | **Implemented** — the key policy now enumerates explicit admin actions (no `kms:*` wildcards). The account-root `Principal` is intentional for a sample (the deploy role is not known in advance); scope it to your deployment role for production. `Resource: '*'` in a key policy is correct — it is self-referencing. |
| 4 | MSKBootstrap / AthenaTableCreator Lambdas ran outside the VPC; MSKBootstrap had no reserved concurrency | **Implemented** — both now run in the private subnets behind the Lambda security group (control-plane calls egress via the NAT gateway), and MSKBootstrap has `ReservedConcurrentExecutions: 2`. |

Additional hardening applied:

| # | Item | Status |
|---|------|--------|
| 5 | KMS-encrypt CloudWatch log groups that may hold sensitive data | **Implemented** — the MSK broker and VPC flow log groups are encrypted with the stack KMS key (key policy grants `logs.{region}.amazonaws.com` scoped by encryption context). |
| 6 | RDS backup retention ≥ 7 days; S3 Object Lock on Delta/Quarantine; inline→managed policies | **Addressed** — `BackupRetentionPeriod` raised to 7. The other two are deliberate design decisions, not outstanding work: S3 Object Lock is incompatible with this workload (Delta Lake MERGE/VACUUM must delete files, and `teardown.sh` must empty buckets), so it belongs only on production buckets with regulatory retention needs. Inline IAM policies are kept because each role's permissions stay beside it in one reviewable template — managed policies pay off at multi-stack scale, not in a single-stack sample. |

## Static Analysis Triage

Disposition of every finding class from a full SAST scan of this repository.
Four kics queries triaged as by-design are disabled via
[`.gitlab/sast-ruleset.toml`](.gitlab/sast-ruleset.toml) (with per-query
rationale in that file); all other queries remain active so real regressions
are still caught.

| Finding | Severity | Disposition |
|---------|----------|-------------|
| Use of cryptographically weak PRNG (CWE-338), ~140 hits in `examples/*/scripts/data_generator.py` | Low | **Fixed** — generators now use `random.SystemRandom()` (OS entropy, identical API). The values are synthetic test data with no cryptographic purpose; the change exists to keep scans clean. |
| Security Group Ingress With Port Range (×2) | Medium | **Accepted, suppressed inline** — both are SELF-referencing rules (source = the same SG, no CIDR exposure). The Glue rule is an [AWS Glue requirement](https://docs.aws.amazon.com/glue/latest/dg/setup-vpc-for-glue-access.html) for VPC jobs (Spark shuffle uses dynamic ports); the MSK rule covers Kafka inter-broker ports. |
| S3 Bucket Logging Disabled (access-logs bucket) | Medium | **Accepted, suppressed inline** — the flagged bucket is the server-access-logging destination for every other bucket; logging it to itself would loop. All data buckets log to it. |
| IAM policy allows for data exfiltration (×7) | Medium | **Accepted** — heuristic flags any policy containing read actions (`s3:GetObject`, `glue:GetTable`, `secretsmanager:GetSecretValue`, …). Every flagged policy is scoped to this stack's own resources (stack buckets, `${GlueDatabaseName}`, stack secrets); the pipeline cannot function without reading them. `Resource: '*'` appears only where AWS does not support resource-level permissions (see Accepted Security Debt table). |
| VPC Without Network Firewall | Medium | **Accepted** — demo scope; see Production Hardening Recommendations. Security groups + private subnets + TLS-only bucket policies are the compensating controls. |
| Lambda: no DLQ / no X-Ray / no tags (×5 functions) | Info | **Accepted** — utility Lambdas are invoked synchronously by deploy scripts (DLQs apply to async invocations); X-Ray and tagging are production niceties for a sample. |
| RDS: 1-day backup retention, deletion protection off | Info | **Accepted** — demo cost/teardown trade-off, documented inline in the template. `AutoMinorVersionUpgrade` and `CopyTagsToSnapshot` are enabled. |
| RouterTable default routing (0.0.0.0/0 via NAT/IGW) | Info | **Accepted** — this is the standard public/private subnet pattern; private subnets route through NAT only. |
| Shield Advanced / IAM Access Analyzer not in use | Info | **Accepted** — account-level services out of scope for a per-stack sample template. |

## Production Hardening Recommendations

For production deployments, consider the following additional measures:

- Enable Amazon RDS deletion protection and Multi-AZ (Multi-AZ is enabled in this template)
- Add AWS Glue security configuration for job bookmark encryption
- Encrypt Lambda environment variables with a customer-managed KMS key
- Add AWS Network Firewall or VPC endpoint policies for additional network controls
- Enable AWS CloudTrail for API-level auditing
- Implement automated secret rotation for Amazon RDS and Amazon MSK credentials
- Add AWS WAF if exposing any API endpoints
- Review and tighten IAM policies for your specific use case
