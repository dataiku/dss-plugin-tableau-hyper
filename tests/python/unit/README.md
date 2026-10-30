# Test procedure

Running the unit tests locally:
```bash
make unit-tests
```

The Python uploader tests use the real DSS SDK. The Makefile adds
`$DKUINSTALLDIR/python` to `PYTHONPATH` by default.