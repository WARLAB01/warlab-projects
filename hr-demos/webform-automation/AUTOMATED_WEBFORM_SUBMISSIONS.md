# AUTOMATED WEBFORM SUBMISSIONS

## Overview

This task automated the bulk submission of new-hire and employee provisioning requests into the Vantrax HCM Workforce Operations Platform. A roster of employee records supplied as a CSV was loaded, authenticated against the tenant, and each record was filed end-to-end through the platform's New Provisioning Request workflow without manual intervention.

## Scope

- **Target system:** Vantrax HCM (Production Demo Tenant, `acme-prod-01`)
- **Authenticated user:** Wes Barlow (HR Operations · Admin)
- **Source data:** `vantrax_employees_v1.csv` (20 employee records)
- **Output:** One Vantrax provisioning request per employee, each returning a `PRV-2026-XXXXXX` reference number

## What Was Accomplished

1. Authenticated to the Vantrax HCM tenant using the supplied service-account credentials.
2. Inspected the New Provisioning Request form to map every field, dropdown, equipment checkbox, and service checkbox to the corresponding CSV column.
3. Submitted a controlled test order (Marcus Chen) end-to-end to validate the field mapping and confirmation flow.
4. Iterated through the remaining 19 employee records, populating all employee, request, equipment, service, priority, approver, delivery-address, and notes fields, then submitting each request.
5. Captured the system-issued reference number for every successful submission and compiled them into a single audit log.

## Results

- **Records processed:** 20 of 20
- **Successful submissions:** 20
- **Failures:** 0
- **Items flagged for follow-up:**
  - Filename discrepancy: the request referenced `vantrax_employees_v2.csv` but the attached file was `vantrax_employees_v1.csv`.
  - Three requests submitted at elevated priority (1 Expedited, 2 Critical) that will require timely approver review.
  - Five international shipping addresses (Tokyo, Singapore, Dublin, London, Paris) accepted by the form but worth a logistics double-check.
  - Two requests intentionally contained only equipment or only services (Replacement Equipment and Additional Access request types).

## Artifacts Created

- `vantrax_submission_log.csv` — Full audit log mapping each `employee_id` to its issued Vantrax `reference_number` and submission status.
- `AUTOMATED_WEBFORM_SUBMISSIONS.md` — This summary document.
