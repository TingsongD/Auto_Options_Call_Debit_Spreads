# Vendor data-rights probe checklist — M0-02

Evidence to obtain **in writing** before any paid historical data is downloaded
or transmitted to an external model provider. Attach provider responses and the
applicable agreements to this repository's private evidence store (never commit
licensed data or contracts to git).

## Coverage probe (ThetaData or successor)

- [ ] Exact accessible SPXW quote window (start/end dates) at the subscribed tier
- [ ] Index history entitlement and its start date (documented separately)
- [ ] Greeks availability, methodology, and timestamps
- [ ] Underlying (SPX index) series availability at one-minute resolution
- [ ] Rate-of-access limits for bulk historical download
- [ ] Sample requests returning the documented fields and timestamp semantics

## Rights confirmation

- [ ] Local storage/retention terms, including after subscription changes
- [ ] Developer (non-display, research) use permitted under the licence class
- [ ] Commercial vs. personal-use classification of this project
- [ ] Written permission to transmit raw or derived data to the model provider
- [ ] Provider terms/version archived with a checksum and retrieval date

## Macro sources

- [ ] FRED/ALFRED vintage access confirmed; release-time conventions recorded
- [ ] Fed original-release timestamps established for the study period
- [ ] Document archive (statements/minutes/projections) release times per type

## Signoff

| Item | Evidence location | Confirmed by | Date |
|---|---|---|---|
| Coverage | | | |
| Rights | | | |
| Macro timing | | | |
