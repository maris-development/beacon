---
description: Beacon's authentication and role-based access control — a single config-defined super-user, read-only SQL-managed users and roles, table/path grants and denies with deny-wins semantics, anonymous access, and optional OIDC.
---

# Authentication & Access Control

Beacon has a built-in authentication and **role-based access control (RBAC)**
layer. Authentication answers *who is this principal and what roles do they have*;
authorization — which Beacon owns entirely — answers *what may they read*. The two
are deliberately separable, so an external identity provider (OIDC) can supply
identities while grants stay expressed in Beacon's own role model.

## The model at a glance

- **One super-user**, defined entirely by configuration (`BEACON_ADMIN_USERNAME` /
  `BEACON_ADMIN_PASSWORD`). It is the only principal that can write — run DDL/DML,
  manage users and roles, and call the admin endpoints — and it **bypasses
  authorization** entirely.
- **Users and roles created through SQL are always read-only.** There is no way to
  create a second super-user or grant write privileges to a role; the super-user
  is a fixed credential, never a stored user.
- **Authorization governs reads.** Grants and denies are attached to roles and
  evaluated when a query scans a table or files. Writes and management are not
  role-grantable — they require the super-user.
- **Deny-wins, default-deny.** When enforcement is on, a read is allowed only if a
  matching grant exists and no matching deny does.

## Principals

| Principal | How it authenticates | Capabilities |
| --- | --- | --- |
| **Super-user** | `BEACON_ADMIN_*` credentials, checked directly (constant-time) | Full read + write + management; bypasses authorization |
| **User** | Local username/password, or an OIDC token | Read-only, limited by the roles assigned to it |
| **Anonymous** | No credentials (when enabled) | Read-only, limited by the roles assigned to the `anonymous` user |

Local users are stored in a SQLite directory database (`users/directory.db` under
the data directory), with passwords hashed using Argon2. The super-user is **not**
in this store — it is a fixed credential sourced from the environment, so it can't
be altered or duplicated through SQL.

## Enforcement

Authorization is **off by default** so a fresh deployment is open and easy to try.
Turn it on with:

```bash
BEACON_AUTH_ENFORCE=true
```

| `BEACON_AUTH_ENFORCE` | Behaviour |
| --- | --- |
| `false` *(default)* | Authorization is a no-op. Any principal that can authenticate (including anonymous, if enabled) can read everything. |
| `true` | Default-deny. Every table/file read must be allowed by a grant on the principal's roles, with no overriding deny. The super-user still bypasses all checks. |

Only **reads** (table and file scans) are subject to grant evaluation. All writes
and DDL/DML are gated separately and always require the super-user.

## Roles, privileges and targets

A **role** holds a set of **grant** and **deny** rules. A rule is a *privilege*
optionally scoped to a *target*.

| Privilege | Meaning for read enforcement |
| --- | --- |
| `SELECT` | Read the target. This is the privilege that matters for query authorization. |
| `INSERT`, `UPDATE`, `DELETE`, `CREATE`, `DROP` | Accepted by the grammar for completeness, but **writes require the super-user** — granting them to a role does not confer write access. |
| `ALL` | Matches any privilege. |

| Target | Matches |
| --- | --- |
| `ON TABLE <name>` | A registered table accessed by name. |
| `ON PATH '<glob>'` | Files accessed by path, relative to the datasets root. Supports glob patterns (e.g. `argo/**/*.nc`). |
| *(omitted)* | Every target for that privilege. |

## Managing users and roles (SQL)

All of the statements below are **super-user-only** management DDL. Submit them
over any SQL surface — `POST /api/query` (with `BEACON_ENABLE_SQL=true`) or Arrow
Flight SQL.

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

Remove a rule with `REVOKE`. Use the `DENY` keyword to remove a *deny* rule rather
than a *grant*:

```sql
REVOKE SELECT ON TABLE observations FROM ROLE reader;
REVOKE DENY SELECT ON PATH 'argo/restricted/*' FROM ROLE reader;
```

### Anonymous access

When `BEACON_AUTH_ANONYMOUS_ENABLED=true` (the default), unauthenticated requests
resolve to the built-in `anonymous` user. Give anonymous callers read access by
assigning roles to it like any other user:

```sql
CREATE ROLE public_reader;
GRANT SELECT ON TABLE observations TO ROLE public_reader;
GRANT ROLE public_reader TO USER anonymous;
```

Set `BEACON_AUTH_ANONYMOUS_ENABLED=false` to require authentication for every
request.

## OIDC (single sign-on)

Beacon can validate **OIDC bearer tokens** in addition to local passwords. When
enabled, a request carrying a `Bearer <jwt>` token is validated against the
issuer's JWKS; the username and role names are read from token claims. Beacon
still owns authorization — the token supplies *identity and role names*, and the
grants for those roles live in Beacon's role model.

```bash
BEACON_OIDC_ENABLED=true
BEACON_OIDC_ISSUER=https://keycloak.example.com/realms/beacon
BEACON_OIDC_JWKS_URL=https://keycloak.example.com/realms/beacon/protocol/openid-connect/certs
BEACON_OIDC_AUDIENCE=beacon                      # optional; validated when set
BEACON_OIDC_ROLES_CLAIM=realm_access.roles       # default
BEACON_OIDC_USERNAME_CLAIM=preferred_username    # default
```

A role named in a token only confers access if it has been created and granted in
Beacon (`CREATE ROLE <name>` + `GRANT … TO ROLE <name>`). OIDC principals, like
all non-super-user principals, are read-only.

### How a token is validated

When a request carries `Authorization: Bearer <jwt>`, Beacon validates it as a
JWT access token:

1. **Read the header.** The token's JOSE header is decoded to read its key id
   (`kid`) and signing algorithm (`alg`). A token without a `kid` is rejected.
2. **Resolve the signing key.** The issuer's JWKS (fetched from
   `BEACON_OIDC_JWKS_URL`) is searched for a key matching the `kid`. If no key
   matches, the token is rejected — re-fetching only happens on the cache TTL (see
   below), so brand-new signing keys become usable once the cache expires.
3. **Verify signature and claims.** The signature is checked with the matched key
   using the token's own algorithm (whatever the JWK supports — RS256, ES256, …).
   Expiry (`exp`) and the issuer (`iss`, against `BEACON_OIDC_ISSUER`) are always
   validated. The audience (`aud`) is validated **only when** `BEACON_OIDC_AUDIENCE`
   is set; otherwise audience checking is disabled.
4. **Extract identity and roles** from the claims (below).

Any failure along the way is an authentication failure: a request that **presents**
an invalid or expired token is rejected with `401 Unauthorized` — it does **not**
fall back to anonymous. Anonymous access applies only when a request carries **no**
credentials at all.

### Username and roles claims

Both `BEACON_OIDC_USERNAME_CLAIM` and `BEACON_OIDC_ROLES_CLAIM` are **dotted
paths**, resolved against the (possibly nested) claims object — e.g.
`realm_access.roles` reads `claims["realm_access"]["roles"]`.

- The **username** claim must resolve to a string; a token missing it is rejected.
- The **roles** claim is optional and accepts either shape commonly seen in OIDC
  tokens: a **JSON array of strings** (`["reader", "writer"]`) or a single
  **space-delimited string** (`"reader writer"`). A missing or non-string/array
  roles claim simply yields **no roles** (not an error) — the principal
  authenticates but, under enforcement, can read nothing until granted roles.

For a Keycloak realm, the defaults (`realm_access.roles` and
`preferred_username`) work out of the box.

### JWKS caching

The JWKS document is fetched lazily and cached in memory for
`BEACON_OIDC_JWKS_CACHE_TTL_SECS` (default `300`). Within that window, token
validation does no network I/O; after it, the next validation re-fetches. The
cache lock is never held across the network fetch, so token validation doesn't
serialize behind a slow JWKS endpoint.

### Credential routing (Basic vs Bearer)

When OIDC is enabled, Beacon runs a **composite** provider that routes by
credential kind:

| Request header | Routed to | Used for |
| --- | --- | --- |
| `Authorization: Basic …` | Local provider (SQLite user store) | The super-user and SQL-managed users |
| `Authorization: Bearer …` | OIDC provider | External IdP tokens |

So local admin users and external IdP users coexist. User-management DDL
(`CREATE USER`, `DROP USER`, …) always targets the **local** directory — the OIDC
provider holds no user store, because OIDC users live in your identity provider,
not in Beacon. (The super-user credential is still checked directly and
short-circuits before either provider runs.)

## Configuration reference

| Variable | Default | Description |
| --- | --- | --- |
| `BEACON_ADMIN_USERNAME` | `beacon-admin` | Super-user username. **Change in production.** |
| `BEACON_ADMIN_PASSWORD` | `beacon-password` | Super-user password. **Change in production.** |
| `BEACON_AUTH_ENFORCE` | `false` | Enforce read authorization (default-deny). |
| `BEACON_AUTH_ANONYMOUS_ENABLED` | `true` | Allow unauthenticated requests as the `anonymous` user. |
| `BEACON_OIDC_ENABLED` | `false` | Accept OIDC bearer tokens. |
| `BEACON_OIDC_ISSUER` | _(none)_ | Expected token issuer. |
| `BEACON_OIDC_JWKS_URL` | _(none)_ | JWKS endpoint used to validate token signatures. |
| `BEACON_OIDC_AUDIENCE` | _(none)_ | Expected audience; validated only when set. |
| `BEACON_OIDC_ROLES_CLAIM` | `realm_access.roles` | Token claim (dot-path) holding role names. |
| `BEACON_OIDC_USERNAME_CLAIM` | `preferred_username` | Token claim holding the username. |
| `BEACON_OIDC_JWKS_CACHE_TTL_SECS` | `300` | How long to cache the issuer's JWKS. |

::: tip Transport authentication
Over HTTP, credentials are sent with **Basic auth** (or a `Bearer` token for
OIDC). Arrow Flight SQL authenticates over its handshake and issues a bearer
token; see the [Flight SQL settings](/docs/1.8.0/data-lake/configuration#arrow-flight-sql).
:::
