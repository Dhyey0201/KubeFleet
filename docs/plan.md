# Implementation plan

Follow specification Sections 33 and 39 in order. No milestone is complete merely because source exists.

- [x] M0: native compatibility gate and project toolchain verified; later dependency locks remain release-incomplete
- [x] M1: API/safe-materialization gate passed, including real API race/collision/conflict checks and real worker smoke; T03 CLI belongs to M5 and T11 crash injection remains in M2
- [x] M2: lifecycle/crash/failover/parallel quota, new-UID/persistence/idle and SIGTERM scenarios passed
- [x] M3: real profiles/preemption/reclaim passed; per-victim CPU lower bound measured with explicit missing-final-sample limit
- [x] M4: production gate/crash and real API concurrency/trust scenarios passed
- [x] M5: CLI/security/monitoring/diagnostics passed; fresh setup and public demo reproduced
- [ ] M6: five smoke mixes/15 comparisons/180 successes and requested-CPU intervals passed; 100/500 management load/cleanup passed; full repetitions blocked by host memory
- [ ] M7: local source checks, CI/docs exist; remote CI/fresh setup/full release evidence incomplete
