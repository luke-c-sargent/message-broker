"""Lease smoke test: starts the broker with a short LEASE_SECONDS and drives it over HTTP."""
import http.client, json, os, socket, subprocess, sys, threading, time

PORT, DOM, TTL = 18080, "t.example", 2
SRV = "a" * 32


def call(host, method, path, headers=None, body=None):
    c = http.client.HTTPConnection("127.0.0.1", PORT, timeout=30)
    c.request(method, path, json.dumps(body) if body else None, {"Host": f"{host}.{DOM}", **(headers or {})})
    r = c.getresponse()
    data = r.read()
    return r.status, (json.loads(data) if data and r.getheader("Content-Type", "").startswith("application/json") else data)


env = {**os.environ, "BASE_DOMAIN": DOM, "MASTER_SECRET": "m", "PORT": str(PORT), "LEASE_SECONDS": str(TTL)}
p = subprocess.Popen([sys.executable, os.path.join(os.path.dirname(os.path.abspath(__file__)), "broker.py")], env=env)
try:
    for _ in range(50):
        try:
            call(SRV, "GET", "/alive"); break
        except OSError:
            time.sleep(0.1)

    def register():
        return call(SRV, "POST", "/register", {"X-Master-Secret": "m"})

    st, body = register(); assert st == 200, st
    H = {"X-Run-Secret": body["run_secret"]}
    st, body = call(SRV, "POST", "/mint", H, {"n": 1}); assert st == 200
    cli = body["tokens"][0]
    assert call(SRV, "GET", "/status", H)[0] == 200
    assert register()[0] == 409

    # heartbeats keep the run alive past one TTL; /alive works for server and client, no secret
    for _ in range(4):
        time.sleep(TTL / 2)
        st, b = call(SRV, "POST", "/heartbeat", H); assert st == 200 and 0 < b["remaining"] <= TTL, (st, b)
    assert call(SRV, "GET", "/alive")[0] == 200 and call(cli, "GET", "/alive")[0] == 200
    # polling /alive alone must not keep the run alive (it would never reach 410 if it renewed)
    t0 = time.time()
    while call(SRV, "GET", "/alive")[0] == 200 and time.time() - t0 < TTL + 3:
        time.sleep(0.3)
    assert call(SRV, "GET", "/alive")[0] == 410 and call(cli, "GET", "/alive")[0] == 410
    assert call(SRV, "GET", "/status", H)[0] == 403 and call(SRV, "GET", "/next", H)[0] == 403
    assert call(cli, "POST", "/v1/fleet/get-run")[0] == 404
    assert call(SRV, "POST", "/revoke", H)[0] == 204  # idempotent after expiry
    st, body = register(); assert st == 200  # slot is free again
    H = {"X-Run-Secret": body["run_secret"]}

    # idle run, no traffic but one blocked /next: the sweep must expire it and wake the poll
    t0 = time.time()
    assert call(SRV, "GET", "/next", H)[0] == 403 and time.time() - t0 < TTL + 3
    st, body = register(); assert st == 200
    H = {"X-Run-Secret": body["run_secret"]}

    # pending /next is woken when the run is revoked
    t0 = time.time()
    threading.Timer(1, lambda: call(SRV, "POST", "/revoke", H)).start()
    assert call(SRV, "GET", "/next", H)[0] == 403 and time.time() - t0 < 5
    assert call(SRV, "POST", "/revoke", H)[0] == 204 and call(SRV, "GET", "/alive")[0] == 410
    assert call("f" * 32, "GET", "/alive")[0] == 410  # unknown token

    # Fleet relay end to end: client -> broker -> fake wrapper (/next, /reply) -> client
    st, body = register(); assert st == 200
    H = {"X-Run-Secret": body["run_secret"]}
    cli = call(SRV, "POST", "/mint", H, {"n": 1})[1]["tokens"][0]

    def wrapper():
        while True:
            c = http.client.HTTPConnection("127.0.0.1", PORT, timeout=30)
            c.request("GET", "/next", headers={"Host": f"{SRV}.{DOM}", **H}); r = c.getresponse(); r.read()
            if r.status == 200:
                call(SRV, "POST", "/reply/" + r.getheader("X-Req-Id"), {**H, "X-Status": "200", "X-Resp-Headers": "{}"})
                return

    th = threading.Thread(target=wrapper); th.start(); time.sleep(0.5)
    assert call(cli, "POST", "/v1/fleet/get-run")[0] == 200
    th.join()

    # a /next poll whose wrapper hung up must not swallow the next client request
    dead = socket.create_connection(("127.0.0.1", PORT))
    dead.sendall(f"GET /next HTTP/1.1\r\nHost: {SRV}.{DOM}\r\nX-Run-Secret: {H['X-Run-Secret']}\r\n\r\n".encode())
    time.sleep(0.5); dead.close(); call(SRV, "POST", "/heartbeat", H)  # handler keeps waiting on the queue, first in line
    th = threading.Thread(target=wrapper); th.start(); time.sleep(0.5); call(SRV, "POST", "/heartbeat", H)  # the live poll, second in line
    t0 = time.time()
    assert call(cli, "POST", "/v1/fleet/get-run")[0] == 200 and time.time() - t0 < 5  # else 504 after 60s
    th.join()
    assert call(SRV, "POST", "/revoke", H)[0] == 204
    print("ok")
finally:
    p.terminate()
