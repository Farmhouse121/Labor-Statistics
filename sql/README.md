# Snapshot of Database Tables
This contains a snapshot of the relevant database tables, run as of 2022-03-03, which will create them all and fill them via the command...
`mysql database_name < snapshot.sql`
As of now, there's a gap in the data for the CPI weights (section W% that is not actually part of the official LABSTAT database, it is populated by my `getweights.py` code). I may fix that and reload this snapshot.
