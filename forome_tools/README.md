The package **forome_utils** includes various (small) tools that can be
used in wide spectrum of Python programms.

Main subjects:

* Processing JSON objects and JSON record list files(each line is a recordin JSON format)
* HTTP-service tools for back-end support

cmpjson.py
==========
The autonomous utility provides solution for the following problem.

There are two  versions of output for the same original data with
format of JSON record list (each line is a record in JSON format). We need to
compare versions in an essential way but lists are too long.

The utility randomly selects small amount of records (5 by default)
and evaluates difference for this selection.

hserv.py
=======
Implementation of "simple" HTTP-server based on standard Python
libraries. Appears to be sufficient to organize back-end services.

In autonomous mode (for testing or internal purposes) runs
over **wsgiref.simple_server**.
In critical applications runs from uWSGI container.

Configuration schema uses **read_json.py** functionality, see below.

ident.py
=======
Utility function **checkIdentifier(value)** just checks if value is a
correct identifier (in Python, C, Java, ... terms)

ixbz2.py
=======
Blocking archive format support, based on bz2 algorithm.

The problem is as follows. We have long list of records, we need
to store them in a compressed way but with a possibility to
pick up a single record in a controlled period of time.

The solution: to block the records in portions of a controlled length
and to compress each portion separatedly.

The API includes compression and decompression implementation.
An autonomous utility can compress and uncompress data.

job_pool.py
==========
The code provides implementation for job pool inside an application.
Jobs (tasks) are run in specially allocated autonomous threads in
parallel to the main threads of the application.

json_conf.py
==========
Reading of JSON configuration file with support of convenience features:

* full line C-style comments, starting with '//'
* use macro replacements (macro can be defined inside configuration
file as well as to be defined from outside)

log_err.py
========
Just a helper to handle Python exceptions and log them in logging mechanism.

path_works.py
============
Yet another small toolset to fetch data from JSON objects

read_json.py
===========
A tool provides reading JSON record list files (each line is JSON record) in
various of contexts:

* list of files defined by a (star-) pattern
* files can be compressed by gzip, bz2

rest.py
======
REST-call functionality, allows to perform HTTP/HTTPS request and
convert its response  in a JSON-object

sphinx_doc.py
============
A starter for Sphinx Documentation generation

sync_obj.py
==========
Java-style object with syncronization

types.py
=======
A small toolset to count values of different types (used in JSON processing)

variants.py
=========
A helper class to handle enumerated set of string variants
