# Traceability matrix

> Maintained by hand until `scripts/check_traceability.py` (FR-DEV-03) exists, which will
> generate this file from requirement tags in tests. Implementation and test columns are
> empty because no code exists yet (Phase 2).

| Requirement | Pri | Spec | ADR | Implementation | Planned tests | Status |
|---|---|---|---|---|---|---|
| FR-GEN-01 | P0 | 01 | — | — | TS-U-06, TS-I-01 | Specified |
| FR-GEN-02 | P0 | 01, 02 §6 | — | — | TS-U-06 | Specified |
| FR-GEN-03 | P1 | 01, 02 §6 | — | — | TS-D-02 | Specified |
| FR-ING-01 | P0 | 01, 02 §1–2 | — | — | TS-C-02, TS-I-01 | Specified |
| FR-ING-02 | P1 | 01, 02 §1 | — | — | TS-I-01 | Specified |
| FR-VAL-01 | P0 | 01, 02 §2 | — | — | TS-U-01, TS-U-02, TS-C-01, TS-I-02 | Specified |
| FR-VAL-02 | P0 | 01 | 0005 | — | TS-U-03, TS-I-02 | Specified |
| FR-VAL-03 | P0 | 01, 02 §2 | — | — | TS-U-01 | Specified |
| FR-DLQ-01 | P0 | 01, 02 §3 | 0002 | — | TS-I-02 | Specified |
| FR-DLQ-02 | P1 | 01 | — | — | TS-I-02 | Specified |
| FR-STR-01 | P0 | 01, 03 | 0006 | — | TS-I-01 | Specified |
| FR-DED-01 | P0 | 01, 02 §4 | 0002, 0003 | — | TS-I-03, TS-U-05 | Specified |
| FR-DED-02 | P1 | 01 | 0005 | — | TS-P-02 | Specified |
| FR-DED-03 | P1 | 01 | 0003 | — | TS-I-03 | Specified |
| FR-ORD-01 | P0 | 01 | 0004 | — | TS-U-04, TS-U-05, TS-U-07, TS-I-04 | Specified |
| FR-LATE-01 | P1 | 01 | 0005 | — | TS-I-04 | Specified |
| FR-STO-01 | P1 | 01, 02 §5 | 0006 | — | TS-I-05 | Specified |
| FR-STO-02 | P1 | 01, 02 §5 | — | — | TS-D-04 | Specified |
| FR-ANL-01 | P1 | 01, 02 §4 | 0005 | — | TS-D-03, TS-D-04 | Specified |
| FR-ANL-02 | P1 | 01 | — | — | TS-D-03 | Specified |
| FR-REC-01 | P0 | 01, 06 | 0002 | — | TS-R-01, TS-R-03, TS-R-05 | Specified |
| FR-REC-02 | P1 | 01, 03 | 0006 | — | TS-R-04 | Specified |
| FR-REC-03 | P1 | 01, 06 | 0002 | — | TS-R-02 | Specified |
| FR-REC-04 | P2 | 01, 06 | — | — | TS-R-07 | Specified |
| FR-OBS-01 | P1 | 01, 04 | — | — | metric contract test | Specified |
| FR-OBS-02 | P0 | 01, 06 | — | — | TS-U-08 | Specified |
| FR-OBS-03 | P2 | 01 | — | — | — | Specified |
| FR-DEV-01 | P0 | 01, 08 | 0001 | — | CI | Specified |
| FR-DEV-02 | P0 | 01 | — | — | CI | Specified |
| FR-DEV-03 | P1 | 01 | — | — | CI | Specified |
| NFR-01 | — | 04 | 0002 | — | TS-U-05, TS-R-04 | Specified |
| NFR-02 | — | 04 | 0002 | — | TS-R-01..05 | Specified |
| NFR-03 | — | 04 | — | — | TS-R-01, TS-R-06 | Specified |
| NFR-04 | — | 02, 04 | — | — | TS-C-03 | Specified |
| NFR-05 | — | 04 | 0005 | — | TS-P-02 | Specified |
| NFR-06 | — | 04, 06 | — | — | CI (gitleaks) | Specified |
| NFR-07 | — | 04 | 0001 | — | CI | Specified |
| NFR-08 | — | 04 | — | — | maintenance DAG test | Specified |
| NFR-09 | — | 04, 06 | — | — | TS-U-08 | Specified |
| NFR-10 | — | 04, 09 | — | — | TS-P-01 | Specified |
| NFR-11 | — | 04, 08 | — | — | config test | Specified |
| DQ-01..10 | — | 05 | — | — | TS-D-01..04 | Specified |
