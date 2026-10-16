# PostgreSQL migrations

This consumer imports Automation's Drizzle tables and owns their migration
history. Add application tables to `src/schema.ts` as needed.

From the repository root:

```sh
vp install
vp run build
vp -C examples/postgres run db:generate --name=describe_change
DATABASE_URL=postgresql://localhost/automation vp -C examples/postgres run db:migrate
```

Use a new database for this example. Review and commit generated SQL and
snapshots before applying them. Drizzle records applied migrations; application
startup does not generate or push schema changes.

Existing deployments keep their migration history and any application-specific
constraints or column types. Integrate these table definitions deliberately;
do not apply this example's initial migration over existing tables.
