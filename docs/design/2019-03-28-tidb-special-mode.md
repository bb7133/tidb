
# Proposal: A special mode of TiDB server

- Author(s):     [Wang Cong](https://github.com/bb7133)
- Last updated:  2019-04-03

## Note

In MySQL, term *schema* is often [referred to a database](https://dev.mysql.com/doc/refman/8.0/en/glossary.html#glos_schema) rather than its literal meaning. To avoid ambiguity, all *schema*s in this proposal means its literal definition -- the set of structural informations of a database system, and database is referred to by the term *database*.

## Abstract

This document proposes a special mode for TiDB server, which serves as a fall-back option when servers can not start because of some corrupted schema data. The this helps users locating the causes of the corruption, as well as fixing them if the problems are trivial.

## Background

In some corner cases, DDL operations may accidentally corrupt schema data because of unexpected bugs or compatibility issues, causing a panic of TiDB server and failing to start the service. Usually TiDB's end-users can do little with servers that cannot start, it will be helpful if a special mode is given to them. In the special mode, TiDB can bootstrap with minimum requirements and provides functionalities for the users to analyze / fix the corrupted schema manually.

The general purpose of this mode is similar to *safe mode* offered by many operating systems. So maybe *safe mode* is a suitable name for this mode.

## Proposal

1. <span id = "bootstrap">Special bootstrap</span>

    * When bootstrapping, skip loading parts of the tables (**Discussion needed**: use tables in blacklist, or just all users tables - all user tables are preferred because it's just simple without consistency problems) in TiDB's `InfoSchema`.

    * No DDL / DML statement is allowed for those TiDB servers in special mode, servers will not try to be DDL owner neither.

2. HTTP API to locate the cause of corruption

 [TiDB HTTP API](https://github.com/pingcap/tidb/blob/master/docs/tidb_http_api.md) should be still available in special mode, which makes possible to manually check and dump the database / table / column schema for our users.

 However, for now the `schema` part of this service relies `InfoSchema`. As stated in [Bootstrap](#bootstrap) part of this proposal, `InfoSchema` in special mode contains only system tables. (**Discussion needed**)We may need to communicate with PD directly to fetch full schema informations.

3. `ADMIN` Commands in special mode

    As a TiDB extension syntax, `ADMIN` commands can be used to view the status of TiDB, as well as cancel some running DDL jobs. We can provide more commands to help the users checking the schema, and tries to fix the corrupted objects.

	The proposed commands are:

    * `ADMIN CHECK TABLE SCHEMA table_id`: this command checks table schema, including:

        - If table state and all columns states are `public`.

	    - If all charsets / collations of the table and it's columns are valid.

	    - If all indices of the table are valid, for example: maximum indices check.

	    - Other sanity checks, for example, there should be no foreign key constraint because foreign key is not supported by TiDB.

    * `ADMIN OVERWRITE TABLE SCHEMA table_id path_of_table_schema_json`: this command can be used to overwrite the schema of a table. `path_of_table_schema_json` gives the path of a JSON file that can be de-serialize to `model.TableInfo`. It is a powerful command that allows users to manipulate TiDB schema data directly. Before the overwrite applies, some checks need to be done:

        - `ADMIN CHECK TABLE SCHEMA` will be applied firstly to make sure table schema is valid.

		- `SchemaVersion` of the table should not be changed.

	    (**Discussion needed**: If a table schema is overwritten, what should we do to the DDL job history?)

    No DDL job should be running / in job queue when those commands are about to run.

## Rationale

Rather than allowing TiDB server bootstrap in special mode, we can also provide a tool like [tidb-ctl](https://github.com/pingcap/tidb-ctl) to validate / manipulate database schema. Using a individual tool doesn't need a mode that is 'special', which makes a better isolation with current TiDB codes.

## Compatibility

As a new feature, special mode is not supported in old TiDB versions.

## Implementation

1. Special bootstrap

    * A startup flag(`--special-mode`) can be added to the server flag(or configuration variable?) to indicate TiDB server should bootstrap in special mode, during which, `Domain.fetchAllSchemasWithTables` fetch only system table(`mysql`) and generate virtual tables(`INFOMATION_SCHEMA` and `PERFORMANCE_SCHEMA`), those three tables are required by the TiDB server.

    * Before executing a DDL statement, `planner` checks if TiDB is in special mode, and prevent the execution of the statement if so by raising an error message.

2. HTTP API

	To be added.

3. `ADMIN` Commands

    We can define a new `checkTableSchemaExecutor`.
