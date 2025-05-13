
# 📘 CDX Workers' Comp Monolith Roadmap

## 🎯 Objective

Build a single Django-based monolithic system to manage the full lifecycle of Workers’ Comp care: from intake and referral, to scheduling, care coordination, bill review, and reporting — with full transparency for clients and strict control over providers, rates, and reimbursement.

---

## 🧱 1. Core Architecture

### ✅ Monolith Design

- One unified **PostgreSQL** database
- Multiple **Django apps** within a single project
- Each app owns its models and views, but all share the same core entities (`Claim`, `Claimant`, `User`)

---

## 📂 2. Django App Structure

```
cdx_ehr/
├── core/              # shared base models (Claim, Claimant, User)
├── intake/            # referrals and order creation
├── care/              # scheduling, notes, timeline
├── billing/           # provider bills, fee schedule, EOBR
├── network/           # provider registry, automation, rules
├── clients/           # payer registry, adjusters, SLA
├── documents/         # all uploads, contracts, reports, etc.
├── timeline/          # notes, tasks, communication logs
├── reporting/         # analytics, KPIs, exports
├── api/               # future external-facing endpoints
└── manage.py
```

---

## 🗃️ 3. Database Tables by App

### `core/`
- User
- UserProfile
- Claimant
- Claim

### `intake/`
- Referral
- Order

### `care/`
- Appointment
- CareNote
- Task

### `billing/`
- ProviderBill
- BillLineItem
- FeeSchedule
- EOBR
- ReimbursementLog

### `network/`
- Provider
- Location
- ProviderContact
- ProviderContract
- ContractRate
- ProviderAutomationRule

### `clients/`
- Client
- ClientContact
- ClientContract

### `documents/`
- Document
- DocumentType

### `timeline/`
- CoordinationNote
- MessageLog

### `reporting/`
- Derived views and aggregations

---

## 🔁 4. Automation & Provider-Specific Best Practices

### Table: `ProviderAutomationRule`
- provider (FK)
- rule_type (Enum: scheduling, reporting, billing)
- steps (Markdown or JSON)
- automatable (Bool)
- trigger_on (Enum: new_referral, new_appt, etc.)

Used to:
- Prompt staff on “what to do next”
- Auto-create tasks
- Trigger faxes, emails, or later integrations

---

## 📥 5. File Intake → DB Mapping

- OCR or upload parsing
- Map to ProviderBill + BillLineItem
- Link to Claim via claim_number or fuzzy matching
- Create Tasks or Notes for failed mappings

---

## 📤 6. Output: EOB, Invoices, Reports

- Generate EOBR linked to ProviderBill
- Attach to Document (type = EOB)
- Export for client portal
- Track paid/unpaid status in ReimbursementLog

---

## 🔐 7. Permissions & Roles

| Role              | Abilities                                    |
|-------------------|----------------------------------------------|
| Intake Specialist | Create Referrals, Intake Orders              |
| Care Coordinator  | Schedule, Track, Add Notes                   |
| Billing Ops       | Review Bills, Approve/Reject CPTs            |
| Network Manager   | Maintain provider data and rules             |
| Client User       | View only, filtered to their Claimants       |
| Admin             | Everything                                    |

---

## 📈 8. Reporting KPIs (via `reporting/` app)

- TAT: Referral → Appt → Final Report
- Denial rates by CPT/modifier
- Network leakage
- Bill approval %
- SLA breaches

---

## 🛠 9. Build Strategy in Cursor

### Phase-by-Phase Plan

| Phase | Focus                             | Apps Involved             |
|-------|-----------------------------------|---------------------------|
| 1     | Referrals + Orders + Appointments | core, intake, care        |
| 2     | Providers + Automation Rules      | network                   |
| 3     | Bill Parsing + EOB + Rates        | billing                   |
| 4     | Docs + Timeline + Reporting       | documents, timeline, reporting |
| 5     | Client Dashboard + Access         | clients, api              |

Use `management/commands/` for automation like:
- Bill validation
- OCR ingestion
- Provider task triggers
- SLA enforcement

---

## ✍️ Next Steps

1. Start with backend-first build (models, commands, validation)
2. Begin from your existing `bill_review` logic and expand it
3. Add `core/`, `intake/`, and `care/` slowly
4. Use this roadmap to guide you app-by-app in Cursor

