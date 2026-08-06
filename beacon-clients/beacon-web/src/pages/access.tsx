import * as React from "react";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { Loader2, Plus, Shield, Trash2, UserPlus, X } from "lucide-react";

import type { AuthRole, AuthRule, AuthUser, PrivilegeTarget } from "@beacon/client";
import { useBeacon } from "@/lib/beacon-context";
import { errorMessage } from "@/lib/errors";
import { PageContainer } from "@/components/app-shell";
import { InfoBanner } from "@/components/info-banner";
import { Card } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Badge } from "@/components/ui/badge";
import { Label } from "@/components/ui/label";
import { NativeSelect } from "@/components/ui/native-select";
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog";

/** Render a rule's target as a readable label. */
function targetLabel(rule: AuthRule): string {
  if (rule.target_type === "table") return `TABLE ${rule.target_value}`;
  if (rule.target_type === "path") return `PATH ${rule.target_value}`;
  return "all targets";
}

/** Map a rule back to the SDK's PrivilegeTarget shape (for revoke). */
function ruleTarget(rule: AuthRule): PrivilegeTarget {
  if (rule.target_type === "table") return { type: "table", value: rule.target_value ?? "" };
  if (rule.target_type === "path") return { type: "path", value: rule.target_value ?? "" };
  return { type: "all" };
}

export function AccessPage() {
  const beacon = useBeacon();
  const qc = useQueryClient();
  const [error, setError] = React.useState<string | null>(null);

  const usersQuery = useQuery({ queryKey: ["auth-users"], queryFn: () => beacon.admin.listAuthUsers() });
  const rolesQuery = useQuery({ queryKey: ["auth-roles"], queryFn: () => beacon.admin.listAuthRoles() });

  // One mutation runner for every action: run the thunk, then refresh both lists.
  const action = useMutation({
    mutationFn: (fn: () => Promise<void>) => fn(),
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: ["auth-users"] });
      qc.invalidateQueries({ queryKey: ["auth-roles"] });
      setError(null);
    },
    onError: (e) => setError(errorMessage(e)),
  });
  const run = (fn: () => Promise<void>) => action.mutate(fn);
  const busy = action.isPending;

  const users = usersQuery.data ?? [];
  const roles = rolesQuery.data ?? [];
  const roleNames = roles.map((r) => r.name);

  return (
    <PageContainer
      title="Users & roles"
      description="Manage users, roles, and read access (deny wins over grant)."
    >
      <InfoBanner>
        Roles hold <strong>read</strong> grants only (write access is reserved to the super-user).
        Grant <code>SELECT</code> on a table, a path glob, or all targets; a <strong>deny</strong>{" "}
        always overrides a grant. Assign roles to users to give them access. The super-user is
        defined by <code>BEACON_ADMIN_*</code> and can&rsquo;t be edited here.
      </InfoBanner>

      {error && (
        <div className="mb-3 rounded-md border border-destructive/40 px-3 py-2 text-sm text-destructive">
          {error}
        </div>
      )}

      <UsersCard
        users={users}
        loading={usersQuery.isLoading}
        roleNames={roleNames}
        busy={busy}
        run={run}
        onError={setError}
      />

      <div className="h-4" />

      <RolesCard roles={roles} loading={rolesQuery.isLoading} busy={busy} run={run} />
    </PageContainer>
  );
}

// ---------------------------------------------------------------------------
// Users
// ---------------------------------------------------------------------------

function UsersCard({
  users,
  loading,
  roleNames,
  busy,
  run,
  onError,
}: {
  users: AuthUser[];
  loading: boolean;
  roleNames: string[];
  busy: boolean;
  run: (fn: () => Promise<void>) => void;
  onError: (msg: string) => void;
}) {
  const beacon = useBeacon();
  const [open, setOpen] = React.useState(false);
  const [username, setUsername] = React.useState("");
  const [password, setPassword] = React.useState("");

  function createUser() {
    run(async () => {
      await beacon.admin.createUser(username.trim(), password);
      setOpen(false);
      setUsername("");
      setPassword("");
    });
  }

  return (
    <Card className="overflow-hidden">
      <div className="flex items-center justify-between border-b px-4 py-3">
        <h2 className="text-sm font-semibold">Users</h2>
        <Button size="sm" className="gap-1.5" onClick={() => setOpen(true)}>
          <UserPlus className="h-4 w-4" /> New user
        </Button>
      </div>

      {loading ? (
        <div className="flex items-center gap-2 p-4 text-sm text-muted-foreground">
          <Loader2 className="h-4 w-4 animate-spin" /> Loading…
        </div>
      ) : (
        <ul className="divide-y">
          {users.map((u) => (
            <li key={u.username} className="flex items-center gap-3 px-4 py-2.5 text-sm">
              <span className="flex w-48 shrink-0 items-center gap-1.5 font-mono">
                {u.is_super_user && <Shield className="h-3.5 w-3.5 text-primary" />}
                {u.username}
              </span>
              {u.is_super_user ? (
                <Badge variant="secondary">super-user · full access</Badge>
              ) : (
                <div className="flex flex-1 flex-wrap items-center gap-1.5">
                  {u.roles.map((role) => (
                    <Badge key={role} variant="muted" className="gap-1 pr-1">
                      {role}
                      <button
                        title={`Remove role ${role}`}
                        disabled={busy}
                        className="rounded hover:text-destructive"
                        onClick={() => run(() => beacon.admin.revokeRoleFromUser(u.username, role))}
                      >
                        <X className="h-3 w-3" />
                      </button>
                    </Badge>
                  ))}
                  {u.roles.length === 0 && (
                    <span className="text-xs text-muted-foreground">no roles</span>
                  )}
                  <AddRoleControl
                    options={roleNames.filter((r) => !u.roles.includes(r))}
                    onAdd={(role) => run(() => beacon.admin.grantRoleToUser(u.username, role))}
                    disabled={busy}
                  />
                  {u.is_anonymous && (
                    <Badge variant="secondary" title="Disable with BEACON_AUTH_ANONYMOUS_ENABLED=false">
                      anonymous · always on
                    </Badge>
                  )}
                </div>
              )}
              {!u.is_super_user && !u.is_anonymous && (
                <Button
                  variant="ghost"
                  size="sm"
                  className="h-7 w-7 shrink-0 p-0 text-destructive hover:bg-destructive/10 hover:text-destructive"
                  title="Delete user"
                  disabled={busy}
                  onClick={() => {
                    if (window.confirm(`Delete user "${u.username}"?`))
                      run(() => beacon.admin.dropUser(u.username));
                  }}
                >
                  <Trash2 className="h-4 w-4" />
                </Button>
              )}
            </li>
          ))}
        </ul>
      )}

      <Dialog open={open} onOpenChange={(o) => !busy && setOpen(o)}>
        <DialogContent>
          <DialogHeader>
            <DialogTitle>New user</DialogTitle>
            <DialogDescription>
              Created users are read-only until you assign them roles.
            </DialogDescription>
          </DialogHeader>
          <div className="space-y-3">
            <div className="space-y-1.5">
              <Label htmlFor="new-username">Username</Label>
              <Input
                id="new-username"
                value={username}
                onChange={(e) => setUsername(e.target.value)}
                placeholder="alice"
                autoComplete="off"
              />
            </div>
            <div className="space-y-1.5">
              <Label htmlFor="new-password">Password</Label>
              <Input
                id="new-password"
                type="password"
                value={password}
                onChange={(e) => setPassword(e.target.value)}
                autoComplete="new-password"
              />
            </div>
          </div>
          <DialogFooter>
            <Button variant="ghost" onClick={() => setOpen(false)} disabled={busy}>
              Cancel
            </Button>
            <Button
              disabled={busy || !username.trim() || !password}
              onClick={() => {
                try {
                  createUser();
                } catch (e) {
                  onError(errorMessage(e));
                }
              }}
            >
              {busy ? <Loader2 className="h-4 w-4 animate-spin" /> : "Create user"}
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>
    </Card>
  );
}

/** A compact "add a role" select + button. */
function AddRoleControl({
  options,
  onAdd,
  disabled,
}: {
  options: string[];
  onAdd: (role: string) => void;
  disabled: boolean;
}) {
  const [value, setValue] = React.useState("");
  if (options.length === 0) return null;
  return (
    <span className="flex items-center gap-1">
      <NativeSelect
        value={value}
        onChange={(e) => setValue(e.target.value)}
        className="h-6 w-28 text-xs"
        disabled={disabled}
      >
        <option value="">+ role…</option>
        {options.map((r) => (
          <option key={r} value={r}>
            {r}
          </option>
        ))}
      </NativeSelect>
      {value && (
        <Button
          variant="outline"
          size="sm"
          className="h-6 px-2 text-[11px]"
          disabled={disabled}
          onClick={() => {
            onAdd(value);
            setValue("");
          }}
        >
          Add
        </Button>
      )}
    </span>
  );
}

// ---------------------------------------------------------------------------
// Roles
// ---------------------------------------------------------------------------

function RolesCard({
  roles,
  loading,
  busy,
  run,
}: {
  roles: AuthRole[];
  loading: boolean;
  busy: boolean;
  run: (fn: () => Promise<void>) => void;
}) {
  const beacon = useBeacon();
  const [newRole, setNewRole] = React.useState("");

  return (
    <Card className="overflow-hidden">
      <div className="flex items-center justify-between gap-3 border-b px-4 py-3">
        <h2 className="text-sm font-semibold">Roles</h2>
        <div className="flex items-center gap-2">
          <Input
            value={newRole}
            onChange={(e) => setNewRole(e.target.value)}
            placeholder="role name"
            className="h-8 w-40"
          />
          <Button
            size="sm"
            className="gap-1.5"
            disabled={busy || !newRole.trim()}
            onClick={() =>
              run(async () => {
                await beacon.admin.createRole(newRole.trim());
                setNewRole("");
              })
            }
          >
            <Plus className="h-4 w-4" /> Add role
          </Button>
        </div>
      </div>

      {loading ? (
        <div className="flex items-center gap-2 p-4 text-sm text-muted-foreground">
          <Loader2 className="h-4 w-4 animate-spin" /> Loading…
        </div>
      ) : roles.length === 0 ? (
        <div className="p-4 text-sm text-muted-foreground">No roles yet.</div>
      ) : (
        <ul className="divide-y">
          {roles.map((role) => (
            <RoleRow key={role.name} role={role} busy={busy} run={run} />
          ))}
        </ul>
      )}
    </Card>
  );
}

function RoleRow({
  role,
  busy,
  run,
}: {
  role: AuthRole;
  busy: boolean;
  run: (fn: () => Promise<void>) => void;
}) {
  const beacon = useBeacon();
  return (
    <li className="px-4 py-3">
      <div className="flex items-center justify-between gap-3">
        <span className="font-mono text-sm font-medium">{role.name}</span>
        <Button
          variant="ghost"
          size="sm"
          className="h-7 gap-1 px-2 text-[11px] text-destructive hover:bg-destructive/10 hover:text-destructive"
          disabled={busy}
          onClick={() => {
            if (window.confirm(`Delete role "${role.name}"? Users lose any access it granted.`))
              run(() => beacon.admin.dropRole(role.name));
          }}
        >
          <Trash2 className="h-3.5 w-3.5" /> Delete role
        </Button>
      </div>

      <div className="mt-2 space-y-1.5">
        <RuleChips
          label="Grants"
          rules={role.grants}
          tone="grant"
          busy={busy}
          onRevoke={(rule) => run(() => beacon.admin.revokePrivilege(role.name, ruleTarget(rule), false))}
        />
        <RuleChips
          label="Denies"
          rules={role.denies}
          tone="deny"
          busy={busy}
          onRevoke={(rule) => run(() => beacon.admin.revokePrivilege(role.name, ruleTarget(rule), true))}
        />
      </div>

      <AddRuleForm
        busy={busy}
        onAdd={(target, deny) => run(() => beacon.admin.grantPrivilege(role.name, target, deny))}
      />
    </li>
  );
}

function RuleChips({
  label,
  rules,
  tone,
  busy,
  onRevoke,
}: {
  label: string;
  rules: AuthRule[];
  tone: "grant" | "deny";
  busy: boolean;
  onRevoke: (rule: AuthRule) => void;
}) {
  return (
    <div className="flex flex-wrap items-center gap-1.5 text-xs">
      <span className="w-12 shrink-0 text-muted-foreground">{label}</span>
      {rules.length === 0 && <span className="text-muted-foreground">—</span>}
      {rules.map((rule, i) => (
        <span
          key={i}
          className={
            "flex items-center gap-1 rounded border px-1.5 py-0.5 font-mono " +
            (tone === "deny"
              ? "border-destructive/40 text-destructive"
              : "border-emerald-600/40 text-emerald-700 dark:text-emerald-400")
          }
        >
          {rule.privilege} · {targetLabel(rule)}
          <button
            title="Revoke"
            disabled={busy}
            className="hover:opacity-70"
            onClick={() => onRevoke(rule)}
          >
            <X className="h-3 w-3" />
          </button>
        </span>
      ))}
    </div>
  );
}

/** Inline form to add a grant/deny rule to a role. */
function AddRuleForm({
  busy,
  onAdd,
}: {
  busy: boolean;
  onAdd: (target: PrivilegeTarget, deny: boolean) => void;
}) {
  const [kind, setKind] = React.useState<"grant" | "deny">("grant");
  const [targetType, setTargetType] = React.useState<"all" | "table" | "path">("all");
  const [value, setValue] = React.useState("");

  const needsValue = targetType !== "all";
  const ready = !needsValue || value.trim().length > 0;

  return (
    <div className="mt-2 flex flex-wrap items-center gap-1.5">
      <NativeSelect
        value={kind}
        onChange={(e) => setKind(e.target.value as "grant" | "deny")}
        className="h-7 w-24 text-xs"
        disabled={busy}
      >
        <option value="grant">Grant</option>
        <option value="deny">Deny</option>
      </NativeSelect>
      <span className="text-xs text-muted-foreground">SELECT on</span>
      <NativeSelect
        value={targetType}
        onChange={(e) => setTargetType(e.target.value as "all" | "table" | "path")}
        className="h-7 w-28 text-xs"
        disabled={busy}
      >
        <option value="all">all targets</option>
        <option value="table">table</option>
        <option value="path">path glob</option>
      </NativeSelect>
      {needsValue && (
        <Input
          value={value}
          onChange={(e) => setValue(e.target.value)}
          placeholder={targetType === "table" ? "table name" : "argo/**/*.nc"}
          className="h-7 w-44 font-mono text-xs"
          disabled={busy}
        />
      )}
      <Button
        variant="outline"
        size="sm"
        className="h-7 px-2 text-[11px]"
        disabled={busy || !ready}
        onClick={() => {
          const target: PrivilegeTarget =
            targetType === "all"
              ? { type: "all" }
              : { type: targetType, value: value.trim() };
          onAdd(target, kind === "deny");
          setValue("");
          setTargetType("all");
          setKind("grant");
        }}
      >
        Add rule
      </Button>
    </div>
  );
}
