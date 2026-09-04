---
description: The authentication and role-based access control of Beacon. One super-user, read-only users and roles, grants and denies, anonymous access and OIDC.
---

# Authentication & Access Control

Beacon has a built-in authentication layer and a **role-based access control
(RBAC)** layer. Authentication answers one question: *who is this principal, and
what roles does it have*. Authorization answers another: *what can it read*.
Beacon owns the authorization. The two layers are separate. An external identity
provider with OIDC can therefore give the identities. The grants stay in the role
model of Beacon.

## The model at a glance

- **One super-user**. The configuration defines it, with `BEACON_ADMIN_USERNAME`
  and `BEACON_ADMIN_PASSWORD`. Only the super-user writes data, runs DDL and DML,
  manages users and roles, and calls the admin endpoints. The super-user also
  **skips authorization**.
- **A user or role from SQL is always read-only.** You cannot create a second
  super-user. You cannot grant write privileges to a role. The super-user is a
  fixed credential. It is never a stored user.
- **Authorization controls reads.** A grant and a deny belong to a role. Beacon
  applies them when a query scans a table or a file. You cannot grant a write or a
  management action to a role. Those actions need the super-user.
- **A deny wins. The default is deny.** With enforcement on, Beacon allows a read
  only with a matching grant and without a matching deny.

## Principals

| Principal | How it authenticates | Capabilities |
| --- | --- | --- |
| **Super-user** | The `BEACON_ADMIN_*` credentials. Beacon checks them directly, in constant time | Full read, write and management. It skips authorization |
| **User** | A local user name and password, or an OIDC token | Read-only. Its roles set the limits |
| **Anonymous** | No credentials, when anonymous access is on | Read-only. The roles of the `anonymous` user set the limits |

Beacon stores the local users in a SQLite directory database. The file is
`users/directory.db`, under the data directory. Beacon hashes each password with
Argon2. The super-user is **not** in this store. It is a fixed credential from the
environment. You therefore cannot change or copy it through SQL.

## Enforcement

Authorization is **off by default**. A new deployment is therefore open and easy
to try. Switch it on with:

```bash
BEACON_AUTH_ENFORCE=true
```

| `BEACON_AUTH_ENFORCE` | Behaviour |
| --- | --- |
| `false` *(default)* | Authorization does nothing. Every principal that authenticates reads everything. This includes the anonymous principal. |
| `true` | The default is deny. A grant on a role of the principal must allow each table and file read. No deny may match. The super-user still skips every check. |

Beacon evaluates a grant for a **read** only. A read is a table scan or a file
scan. Beacon controls the writes, the DDL and the DML separately. They always need
the super-user.

### Metadata schemas are the super-user's

Two schemas describe the server. They hold no user data. A read of either one
needs the super-user. This is **always** true. The value of `BEACON_AUTH_ENFORCE`
does not matter:

| Schema | What it exposes |
| --- | --- |
| `information_schema` | Every catalog, schema, table and column name of the server |
| `beacon.system` | The auth directory with `users` and `roles`. It also holds `query_metrics`: the caller, the end time, the text and the plans of every query. Beacon keeps them across a restart |

The internal `__beacon_*` tables need the super-user too. Another principal gets
`permission denied` on a read. This also holds for `SHOW TABLES`, because
DataFusion rewrites that statement onto `information_schema.tables`.

Beacon builds a catalog listing for those principals instead. `GET /api/tables`,
`GET /api/catalogs`, the metadata commands of Flight SQL and the MCP
`list_tables` tool read the catalog as the engine. They return only what the
caller can see. They show no metadata schema and no internal table. With
enforcement on, they show only the tables with a `SELECT` grant from the roles of
the caller. A principal therefore sees exactly what it can read.

## Roles, privileges and targets

A **role** holds **grant** rules and **deny** rules. A rule is a *privilege*. It
can also name a *target*.

| Privilege | Meaning for read enforcement |
| --- | --- |
| `SELECT` | Read the target. Query authorization uses this privilege. |
| `INSERT`, `UPDATE`, `DELETE`, `CREATE`, `DROP` | The grammar accepts them. A **write still needs the super-user**. A grant of one of these to a role gives no write access. |
| `ALL` | Matches every privilege. |

| Target | Matches |
| --- | --- |
| `ON TABLE <name>` | A registered table, by name. |
| `ON PATH '<glob>'` | Files by path, relative to the datasets root. A glob pattern works, for example `argo/**/*.nc`. |
| *(omitted)* | Every target of that privilege. |

## Manage users and roles (SQL)

Every statement below is management DDL for the **super-user only**. Send it over
any SQL interface. Use `POST /api/query`, with the SQL interface on by default. Or
use Arrow Flight SQL.

### Users

```sql
CREATE USER alice WITH PASSWORD 'secret';
DROP USER alice;
```

### Roles

```sql
CREATE ROLE reader;
DROP ROLE reader;

-- Assign / unassign a role to a user
GRANT ROLE reader TO USER alice;
REVOKE ROLE reader FROM USER alice;
```

### Grants and denies

```sql
-- Allow a role to read a specific table
GRANT SELECT ON TABLE observations TO ROLE reader;

-- Allow a role to read files under a path glob
GRANT SELECT ON PATH 'argo/**/*.nc' TO ROLE reader;

-- Carve out an exception — deny-wins over any matching grant
DENY SELECT ON PATH 'argo/restricted/*' TO ROLE reader;

-- Grant every privilege on every target (still read-only in practice)
GRANT ALL TO ROLE reader;
```

Remove a rule with `REVOKE`. Add the `DENY` keyword to remove a *deny* rule
instead of a *grant*:

```sql
REVOKE SELECT ON TABLE observations FROM ROLE reader;
REVOKE DENY SELECT ON PATH 'argo/restricted/*' FROM ROLE reader;
```

### Anonymous access

With `BEACON_AUTH_ANONYMOUS_ENABLED=true`, the default, Beacon maps a request
without credentials to the built-in `anonymous` user. Give that user read access.
Assign roles to it, like to any other user:

```sql
CREATE ROLE public_reader;
GRANT SELECT ON TABLE observations TO ROLE public_reader;
GRANT ROLE public_reader TO USER anonymous;
```

Set `BEACON_AUTH_ANONYMOUS_ENABLED=false` to make every request authenticate.

## OIDC (single sign-on)

Beacon validates **OIDC bearer tokens** next to local passwords. Switch OIDC on.
Beacon then validates a `Bearer <jwt>` token against the JWKS of the issuer. It
reads the user name and the role names from the token claims. Beacon still owns
the authorization. The token gives the *identity and the role names*. The grants
of those roles live in the role model of Beacon.

```bash
BEACON_OIDC_ENABLED=true
BEACON_OIDC_ISSUER=https://keycloak.example.com/realms/beacon
BEACON_OIDC_JWKS_URL=https://keycloak.example.com/realms/beacon/protocol/openid-connect/certs
BEACON_OIDC_AUDIENCE=beacon                      # optional; validated when set
BEACON_OIDC_ROLES_CLAIM=realm_access.roles       # default
BEACON_OIDC_USERNAME_CLAIM=preferred_username    # default
```

A role in a token gives access only after you create it and grant it in Beacon.
Use `CREATE ROLE <name>` and `GRANT … TO ROLE <name>`. An OIDC principal is
read-only, like every principal that is not the super-user.

### How a token is validated

A request can carry `Authorization: Bearer <jwt>`. Beacon then validates it as a
JWT access token:

1. **Read the header.** Beacon decodes the JSON header of the token. It reads the
   key id (`kid`) and the algorithm (`alg`). Beacon rejects a token without a
   `kid`.
2. **Find the signing key.** Beacon fetches the JWKS of the issuer from
   `BEACON_OIDC_JWKS_URL`. It searches for a key with that `kid`. Without a match,
   Beacon rejects the token. Beacon fetches the JWKS again only after the cache
   TTL. See below. A new signing key therefore works after the cache expires.
3. **Check the signature and the claims.** Beacon checks the signature with the
   key and with the algorithm of the token. The JWK decides the algorithms, such
   as RS256 and ES256. Beacon always checks the expiry time (`exp`) and the issuer
   (`iss`) against `BEACON_OIDC_ISSUER`. Beacon checks the audience (`aud`) **only
   when** you set `BEACON_OIDC_AUDIENCE`. Without that value, Beacon does no
   audience check.
4. **Read the identity and the roles** from the claims. See below.

A failure at any step is an authentication failure. A request with an invalid or
expired token gets `401 Unauthorized`. Beacon does **not** fall back to anonymous.
Anonymous access applies only to a request with **no** credentials.

### Username and roles claims

`BEACON_OIDC_USERNAME_CLAIM` and `BEACON_OIDC_ROLES_CLAIM` are **paths with
dots**. Beacon resolves them against the claims object. That object can be nested.
For example, `realm_access.roles` reads `claims["realm_access"]["roles"]`.

- The **user name** claim must give a string. Beacon rejects a token without it.
- The **roles** claim is optional. It takes two common OIDC shapes. The first is a
  **JSON array of strings**, as in `["reader", "writer"]`. The second is one
  **string with spaces**, as in `"reader writer"`. An absent claim, or a claim of
  another type, gives **no roles**. This is not an error. The principal
  authenticates. With enforcement on, it reads nothing until it gets a role.

The defaults work for a Keycloak realm. Those defaults are `realm_access.roles`
and `preferred_username`.

### JWKS caching

Beacon fetches the JWKS document at the first use. It caches the document in
memory for `BEACON_OIDC_JWKS_CACHE_TTL_SECS` seconds. The default is `300`. Token
validation does no network I/O in that period. After it, the next validation
fetches the document again. Beacon never holds the cache lock during the fetch.
Token validation therefore does not wait for a slow JWKS endpoint.

### Credential routing (Basic vs Bearer)

With OIDC on, Beacon runs a **composite** provider. That provider routes by the
type of the credential:

| Request header | Routed to | Used for |
| --- | --- | --- |
| `Authorization: Basic …` | The local provider, with the SQLite user store | The super-user and the users from SQL |
| `Authorization: Bearer …` | The OIDC provider | A token from an external identity provider |

Local admin users and external users therefore work together. The user management
DDL, such as `CREATE USER` and `DROP USER`, always changes the **local**
directory. The OIDC provider holds no user store, because the OIDC users live in
your identity provider, not in Beacon. Beacon checks the super-user credential
directly. That check runs before both providers.

## Configuration reference

| Variable | Default | Description |
| --- | --- | --- |
| `BEACON_ADMIN_USERNAME` | `beacon-admin` | The user name of the super-user. **Change it in production.** |
| `BEACON_ADMIN_PASSWORD` | `beacon-password` | The password of the super-user. **Change it in production.** |
| `BEACON_AUTH_ENFORCE` | `false` | Enforce the read authorization. The default is deny. |
| `BEACON_AUTH_ANONYMOUS_ENABLED` | `true` | Map a request without credentials to the `anonymous` user. |
| `BEACON_OIDC_ENABLED` | `false` | Accept an OIDC bearer token. |
| `BEACON_OIDC_ISSUER` | _(none)_ | The expected issuer of the token. |
| `BEACON_OIDC_JWKS_URL` | _(none)_ | The JWKS endpoint that validates a token signature. |
| `BEACON_OIDC_AUDIENCE` | _(none)_ | The expected audience. Beacon checks it only when you set it. |
| `BEACON_OIDC_ROLES_CLAIM` | `realm_access.roles` | The token claim with the role names. Use a path with dots. |
| `BEACON_OIDC_USERNAME_CLAIM` | `preferred_username` | The token claim with the user name. |
| `BEACON_OIDC_JWKS_CACHE_TTL_SECS` | `300` | The cache time of the JWKS of the issuer, in seconds. |

::: tip Transport authentication
Over HTTP, a client sends its credentials with **Basic auth**. With OIDC it sends
a `Bearer` token. Arrow Flight SQL authenticates in its handshake. It then issues
a bearer token. See the
[Flight SQL settings](/docs/2.0.0-rc5/server/configuration#arrow-flight-sql).
:::
