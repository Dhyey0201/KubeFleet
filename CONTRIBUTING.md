# Contributing

Read the build specification and requirement matrix. Use the pinned bootstrap and
project environment, make focused changes, and include tests at the layer needed
to prove the behavior. `make verify` runs local source checks; kind tests require
the dedicated project cluster. Retain failing evidence until its cause is resolved.
Generated API files must reproduce exactly. Explain status ownership, UID safety,
security changes and operational consequences in review descriptions. Do not add
benchmark or resume claims without recorded measurement and explicit quality limits.
