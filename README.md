sky-importer
============

## Overview

The Sky importer can be used to bulk import JSON data into a Sky table.


## Usage

To run the importer, you'll need to first download and setup Go.
Once it's up and running, you can run:

```sh
$ go get github.com/skydb/sky-importer
```

To use the importer, first create your table and setup your schema using the Sky API.
Next, simply run the importer with the table and list of files you want to import.

```sh
$ sky-importer -t my_table myfile.json myotherfile.json.gz
```


## Example

Below is an example of importing a simple JSON file named `data.json`:

```json
{"id":"bob", "timestamp":"2013-01-01T00:00:00Z", "name":"Bobby", "action":"/index.html"}
{"id":"bob", "timestamp":"2013-01-01T00:01:00Z", "action":"/about.html"}
{"id":"susy", "timestamp":"2013-01-01T00:01:00Z", "name":"Susy", "action":"/signup.html"}
```

First we'll start the Sky server:

```
$ sudo skyd
```

Then create our table and add the `name` and `action` properties.
The `name` will be a permanent property and the `action` will be a transient property.

```sh
$ curl -X POST http://localhost:8585/tables -d '{"name":"sky-importer-test"}'
$ curl -X POST http://localhost:8585/tables/sky-importer-test/properties -d '{"name":"name","transient":false,"dataType":"string"}'
$ curl -X POST http://localhost:8585/tables/sky-importer-test/properties -d '{"name":"action","transient":true,"dataType":"factor"}'
```

Now we'll run our importer:

```sh
$ sky-importer -t sky-importer-test data.json
```

You should see 3 PATCH calls made on your server window and then the importer will be done.


## Questions

If you have any questions or found some bugs, please send an e-mail to the [Sky Google Group](https://groups.google.com/d/forum/skydb).