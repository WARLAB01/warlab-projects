# HR Datamart — Project Instructions for Claude

## Task Timing Requirement

**ALL hr-datamart changes must be timed and logged.**

When starting any task on this project:
1. Record the UTC start time immediately (`date -u +%Y-%m-%dT%H:%M:%SZ`)
2. Do the work
3. Record the UTC end time when QA passes
4. Compute the duration
5. Append a new entry to `TASK_TIMING_LOG.md` following the existing format (Task N, start/end/duration, scope, files changed)
6. Commit the timing log update as part of the same git commit

## AWS Infrastructure

- **Redshift cluster:** warlab-hr-datamart (us-east-1)
- **Database:** dev
- **Schema:** l3_workday
- **S3 data bucket:** warlab-hr-datamart-dev
- **S3 dashboard bucket:** warlab-hr-dashboard
- **CloudFront:** E3RGFB9ROIS4KH → d142tokwl5q6ig.cloudfront.net
- **Lambda:** warlab-dashboard-extractor
- **Credentials file:** stored in user's `.credentials/service_keys.env`

## Redshift Access

The VM cannot reach Redshift directly (DNS resolution fails). Use one of:
- **AWS Redshift Data API** via MCP tool (`mcp__AWS_API_MCP_Server__call_aws`) for short SQL
- **Desktop Commander** to run `aws redshift-data execute-statement` from the Mac side for long SQL files (write SQL to the mount point first)

## Key Patterns

- **CC-to-DPT bridge:** `REPLACE(supervisory_organization, 'CC', 'DPT') = department_id` for department lookups
- **SCD2 joins:** Always use `BETWEEN valid_from AND valid_to`, never `<=` inequality joins
- **Reference dims:** `valid_from = '2000-01-01'` anchor date, not CURRENT_DATE
- **Boolean casts:** Redshift requires `BOOLEAN::INT::VARCHAR`, not `BOOLEAN::VARCHAR`
- **String concat:** Use `||` operator, not `CONCAT()` with 3+ args

## Git

- Remote: https://github.com/WARLAB01/warlab-projects.git
- Always commit with `Co-Authored-By: Claude Opus 4.6 <noreply@anthropic.com>`
- See `LESSONS_LEARNED.md` for full list of Redshift gotchas
