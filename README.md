## history-mailer

The history mailer is a python script to manage histories on a [Galaxy](https://github.com/galaxyproject/galaxy) instance. It interacts with the Galaxy API and a [Postal](https://github.com/postalserver/postal) API using admin privileges to detect unused histories, alert users on unused histories and delete these if the user does not update their histories. The history mailer maintains a local database to keep track of email notifications sent to users.

The script was designed and written by Simon Gladman @slugger70 and Thom Cuddihy @thomcuddihy.

#### Usage:
```
usage: history_mailer.py [-h] [-d] [-w] [--delete] [--force] [--production] [--notify] [--drop_db] [--purge]

Manage user histories in Galaxy

optional arguments:
  -h, --help    show this help message and exit
  -d, --dryrun  Do a dry run. List affected users, but do not send emails or delete histories
  -w, --warn    Do a history scan and send warning emails to affected user
  --delete      Do a history scan, send emails and delete eligible histories.
  --force       Force a run even if last run was less than configured threshold.
  --production  Act on the production server instead of the staging server by default
  --notify      Post results to Slack
  --drop_db     Drop associated database. Does not do processing.
  --purge       Purges previously deleted histories.
```

#### Configuration

Copy `config.py.sample` to `config.py` and update values. By default, history_mailer.py users config values of a test (staging) server but can run without these values set, if the `--production` flag is used.

#### Setting up local database files
Setting up a local production database:

```
export HISTORY_MAILER_DB=prod_hm.sqlite
alembic current
alembic update head
```

Setting up a local staging database:

```
export HISTORY_MAILER_DB=staging_hm.sqlite
alembic current
alembic update head
```

#### Ansible role

[ansible-history-mailer](https://github.com/usegalaxy-au/ansible-history-mailer)


