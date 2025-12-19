# Fake data

This directory contains scripts to create a fake, minimal dataset that looks like the source data
but can be used for testing.

Create a fake database by providing a DB url to the `fake_db.py` script:

```bash
./fake_db sqlite:///fake_db.sqlite
```

Simulate entries being updated by supplying the `--simulate-updates` flag:

```bash
./fake_db sqlite:///fake_db.sqlite --simulate-updates
```
