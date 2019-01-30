# dhis2-tools.

This is the collection of tools that i use in day to day activities interacting
with dhis.

> Heads up! Its the work in progress.

## DB Credentials.

Store db credentials in `bin/env.json` with the following format

```json
{
  "user": "dhis",
  "host": "localhost",
  "database": "dhis2",
  "password": "dhis",
  "port": 5432
}
```

### Usage.

Prerequsite: install node from v8 since it uses `async/wait` and many recent
`ES6` features.

```shell
    npm install
```

#### EventUpload.

```shell
  node bin/event-transaction --file pathtothefile.csv
```

The structure of the `pathtothefile.csv` should be like this. It uses id so as
to reduce amount of time it takes to import single event.

```csv
storedby,status,executiondate,organisationunitid,programinstanceid,attributeoptioncomboid,programstageid,completedby,completeddate,dataElementid...
```

#### Tei Delete

```shell
  node bin/tei-delete --file pathtothefile.csv
```

The structure of the `pathtothefile.csv` should be like this. It uses id so as
to reduce amount of time it takes to delete trackedentityinstance.

```csv
  trackedentityinstanceid
```

#### Location update

```shell
  node bin/updateLocationTei --file pathtothefile.csv
```

The structure of the `pathtothefile.csv` should be like this. It uses id so as
to reduce amount of time it takes to delete trackedentityinstance.

```csv
  trackedentityinstanceid,organisationunitid
```
