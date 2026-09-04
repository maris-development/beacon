import { beforeEach, describe, expect, it, vi } from "vitest";

import {
  type Connection,
  clearProxySignedOut,
  detectProxySession,
  identityLabel,
  loadConnection,
  makeClient,
  markProxySignedOut,
  proxyProbeEnabled,
  saveConnection,
} from "../src/lib/auth";

const SERVER = "http://beacon.test";

/** A `Storage` stand-in for the node test environment. */
function memoryStorage(): Storage {
  const map = new Map<string, string>();
  return {
    get length() {
      return map.size;
    },
    clear: () => map.clear(),
    getItem: (key: string) => map.get(key) ?? null,
    key: (index: number) => [...map.keys()][index] ?? null,
    removeItem: (key: string) => void map.delete(key),
    setItem: (key: string, value: string) => void map.set(key, String(value)),
  } as Storage;
}

/** Records every request and answers the admin check with `status`. */
function recordingFetch(status: number) {
  const calls: { url: string; headers: Record<string, string> }[] = [];
  const fn = vi.fn(async (url: string | URL | Request, init?: RequestInit) => {
    calls.push({ url: String(url), headers: { ...((init?.headers ?? {}) as Record<string, string>) } });
    return new Response(status === 200 ? "" : '"unauthorized"', { status });
  });
  globalThis.fetch = fn as unknown as typeof fetch;
  return calls;
}

beforeEach(() => {
  globalThis.localStorage = memoryStorage();
  globalThis.sessionStorage = memoryStorage();
});

describe("detectProxySession", () => {
  it("adopts a session when the credential-less admin check succeeds", async () => {
    const calls = recordingFetch(200);

    const conn = await detectProxySession({ sameOrigin: true, url: `${SERVER}/` });

    expect(conn).toEqual({ url: SERVER, mode: "proxy" });
    expect(calls).toHaveLength(1);
    // The probe rides the admin-gated alias, the same gate the UI uses.
    expect(calls[0]!.url).toBe(`${SERVER}/admin/api/admin/check`);
    // No header of our own: the gateway's injected header must arrive untouched.
    expect(calls[0]!.headers).not.toHaveProperty("Authorization");
  });

  it("falls back to the login screen when no gateway authenticates the caller", async () => {
    recordingFetch(401);

    await expect(detectProxySession({ sameOrigin: true, url: SERVER })).resolves.toBeNull();
  });

  it("falls back to the login screen when the caller is not a super-user", async () => {
    recordingFetch(403);

    await expect(detectProxySession({ sameOrigin: true, url: SERVER })).resolves.toBeNull();
  });

  it("returns null when the server is unreachable", async () => {
    globalThis.fetch = vi.fn(async () => {
      throw new TypeError("Failed to fetch");
    }) as unknown as typeof fetch;

    await expect(detectProxySession({ sameOrigin: true, url: SERVER })).resolves.toBeNull();
  });

  it("does not probe from the dev server (different origin)", async () => {
    const calls = recordingFetch(200);

    await expect(detectProxySession({ sameOrigin: false, url: SERVER })).resolves.toBeNull();
    expect(calls).toHaveLength(0);
  });

  it("stops probing after a sign-out, and resumes after a manual sign-in", async () => {
    const calls = recordingFetch(200);

    markProxySignedOut();
    expect(proxyProbeEnabled(true)).toBe(false);
    await expect(detectProxySession({ sameOrigin: true, url: SERVER })).resolves.toBeNull();
    expect(calls).toHaveLength(0);

    clearProxySignedOut();
    expect(proxyProbeEnabled(true)).toBe(true);
    await expect(detectProxySession({ sameOrigin: true, url: SERVER })).resolves.not.toBeNull();
    expect(calls).toHaveLength(1);
  });
});

describe("makeClient", () => {
  it("sends no Authorization header for a proxy session", async () => {
    const calls = recordingFetch(200);

    await makeClient({ url: SERVER, mode: "proxy" }).admin.check();

    expect(calls[0]!.headers).not.toHaveProperty("Authorization");
  });

  it("sends the basic-auth header for a credentials session", async () => {
    const calls = recordingFetch(200);

    await makeClient({
      url: SERVER,
      mode: "credentials",
      username: "admin",
      password: "pw",
    }).admin.check();

    expect(calls[0]!.headers["Authorization"]).toBe(
      `Basic ${Buffer.from("admin:pw").toString("base64")}`,
    );
  });
});

describe("stored connections", () => {
  it("round-trips a credentials connection", () => {
    const conn: Connection = {
      url: SERVER,
      mode: "credentials",
      username: "admin",
      password: "pw",
    };
    saveConnection(conn);

    expect(loadConnection()).toEqual(conn);
  });

  it("reads a value stored before proxy mode existed as a credentials session", () => {
    localStorage.setItem(
      "beacon-web.connection",
      JSON.stringify({ url: SERVER, username: "admin", password: "pw" }),
    );

    expect(loadConnection()).toEqual({
      url: SERVER,
      mode: "credentials",
      username: "admin",
      password: "pw",
    });
  });

  it("rejects a stored value without credentials", () => {
    localStorage.setItem("beacon-web.connection", JSON.stringify({ url: SERVER }));

    expect(loadConnection()).toBeNull();
  });
});

describe("identityLabel", () => {
  it("names the proxy session rather than a user", () => {
    expect(identityLabel({ url: SERVER, mode: "proxy" })).toBe("Proxy session");
    expect(identityLabel({ url: SERVER, mode: "credentials", username: "ada" })).toBe("ada");
    expect(identityLabel(null)).toBe("admin");
  });
});
