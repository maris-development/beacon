"""Per-format tests.

A package rather than a plain directory, so `formats/test_delta.py` and the HTTP suite's
`test_delta.py` can both exist: without this, pytest sees two modules called `test_delta` and
refuses to import the second.
"""
