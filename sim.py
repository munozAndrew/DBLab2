import json
import argparse
import os
import sys
from collections import defaultdict


INITIAL_DB = {"A": 100, "B": 100, "X": 0, "Y": 0}


class TwoPLEngine:
    def __init__(self, db):
        self.db = dict(db)
        self.granted = defaultdict(list)
        self.wait_queue = defaultdict(list) 
        self.txn_writes = defaultdict(dict)
        self.txn_locks = defaultdict(set) 
        self.blocked = {}
        self.wait_for = defaultdict(set)
        self.active = set()
        self.aborted = set()

    def begin(self, txn):
        self.active.add(txn)
        
        return {"result": "OK"}

    def _holders(self, item):
        return {t for t, m in self.granted[item]}

    def _can_grant(self, txn, item, mode):
        for t, m in self.granted[item]:
            if t == txn:
                continue
            if mode == "X" or m == "X":
                return False
        return True

    def _holds_lock(self, txn, item):
        for t, m in self.granted[item]:
            if t == txn:
                return m
        return None

    def _add_grant(self, txn, item, mode):
        old = self._holds_lock(txn, item)
        if old:
            self.granted[item] = [(t, m) for t, m in self.granted[item] if t != txn]
            
        self.granted[item].append((txn, mode))
        self.txn_locks[txn].add(item)

    def _try_lock(self, txn, item, mode):
        if self.wait_queue[item]:
            for wt, wm in self.wait_queue[item]:
                if wt == txn:
                    break
            else:
                if not self._can_grant(txn, item, mode):
                    return False

        if self._can_grant(txn, item, mode):
            self._add_grant(txn, item, mode)
            return True
        return False

    def read(self, txn, item):
        held = self._holds_lock(txn, item)
        
        if held:
            val = self.txn_writes[txn].get(item, self.db.get(item, 0))
            return {"result": "OK", "value": val}

        if self._try_lock(txn, item, "S"):
            val = self.db.get(item, 0)
            return {"result": "OK", "value": val}
        
        else:
            self._add_to_wait(txn, item, "S")
            blockers = self._get_blockers(txn, item, "S")
            return {"result": "BLOCKED", "why": f"waiting lock on {item}", "blockers": blockers}

    def write(self, txn, item, value):
        held = self._holds_lock(txn, item)

        if held == "X":
            self.txn_writes[txn][item] = value
            return {"result": "OK"}

        if held == "S":
            if self._can_grant(txn, item, "X"):
                self._add_grant(txn, item, "X")
                self.txn_writes[txn][item] = value
                return {"result": "OK"}
            else:
                self._add_to_wait(txn, item, "X")
                blockers = self._get_blockers(txn, item, "X")
                return {"result": "BLOCKED", "why": f"waiting X({item}) upgrade", "blockers": blockers}

        if self._try_lock(txn, item, "X"):
            self.txn_writes[txn][item] = value
            return {"result": "OK"}
        else:
            self._add_to_wait(txn, item, "X")
            blockers = self._get_blockers(txn, item, "X")
            return {"result": "BLOCKED", "why": f"waiting for X({item})", "blockers": blockers}


    def _get_blockers(self, txn, item, mode):
        blockers = set()
        for t, m in self.granted[item]:
            if t == txn:
                continue
            if mode == "X" or m == "X":
                blockers.add(t)
            elif mode == "X" and m == "S":
                blockers.add(t)
        return blockers


    def _add_to_wait(self, txn, item, mode):
        for t, m in self.wait_queue[item]:
            if t == txn:
                return
        self.wait_queue[item].append((txn, mode))
        blockers = self._get_blockers(txn, item, mode)
        self.wait_for[txn] = blockers
        self.blocked[txn] = (item, mode)


    def detect_deadlock(self):
        for start in self.wait_for:
            visited = []
            current = start
            seen = set()
            
            while current is not None:
                if current in seen:
                    cycle = []
                    idx = visited.index(current)
                    cycle = visited[idx:] + [current]
                    return cycle
                seen.add(current)
                visited.append(current)
                next_txn = None
                
                for t in self.wait_for.get(current, set()):
                    if t in self.wait_for:
                        next_txn = t
                        break
                    elif t == start: 
                        visited.append(t)
                        idx = visited.index(start)
                        
                        return visited[idx:]
                current = next_txn
        return None


    def abort(self, txn):
        
        self.aborted.add(txn)
        self.active.discard(txn)
        self.txn_writes.pop(txn, None)
        unblocked = self._release_all_locks(txn)
        
        for item in list(self.wait_queue.keys()):
            self.wait_queue[item] = [(t, m) for t, m in self.wait_queue[item] if t != txn]
        self.wait_for.pop(txn, None)
        
        for t in self.wait_for:
            self.wait_for[t].discard(txn)
        self.blocked.pop(txn, None)
        
        return unblocked


    def commit(self, txn):
        for item, val in self.txn_writes.get(txn, {}).items():
            self.db[item] = val
            
        self.txn_writes.pop(txn, None)
        self.active.discard(txn)
        unblocked = self._release_all_locks(txn)
        
        self.wait_for.pop(txn, None)
        
        return unblocked


    def _release_all_locks(self, txn):

        unblocked = []
        items_held = list(self.txn_locks.get(txn, set()))
        
        for item in items_held:
            self.granted[item] = [(t, m) for t, m in self.granted[item] if t != txn]
        self.txn_locks.pop(txn, None)

        for item in items_held:
            new_queue = []
            for wait_txn, wait_mode in self.wait_queue[item]:
                if wait_txn in self.aborted:
                    continue
                
                if self._can_grant(wait_txn, item, wait_mode):
                    self._add_grant(wait_txn, item, wait_mode)
                    self.wait_for.pop(wait_txn, None)
                    self.blocked.pop(wait_txn, None)
                    unblocked.append((wait_txn, item, wait_mode))
                else:
                    new_queue.append((wait_txn, wait_mode))
            self.wait_queue[item] = new_queue

        return unblocked

    def get_state(self):
        return dict(self.db)


class MVCCEngine:
    def __init__(self, db):
        self.ts_counter = 0
        self.versions = defaultdict(list)
        
        for item, val in db.items():
            self.versions[item].append({
                "value": val, "begin_ts": 0, "end_ts": float('inf'), "txn": None
            })
        self.txn_info = {}
        self.active = set()
        self.aborted = set()

    def _next_ts(self):
        self.ts_counter += 1
        return self.ts_counter

    def begin(self, txn):
        ts = self._next_ts()
        self.txn_info[txn] = {"start_ts": ts, "writes": {}}
        self.active.add(txn)
        
        return {"result": "OK", "start_ts": ts}

    def read(self, txn, item):
        info = self.txn_info[txn]
        start_ts = info["start_ts"]

        if item in info["writes"]:
            return {"result": "OK", "value": info["writes"][item]}

        for ver in reversed(self.versions[item]):
            if ver["begin_ts"] <= start_ts and start_ts < ver["end_ts"]:
                return {"result": "OK", "value": ver["value"]}

        return {"result": "OK", "value": 0}

    def write(self, txn, item, value):
        self.txn_info[txn]["writes"][item] = value
        return {"result": "OK"}

    def commit(self, txn):
        info = self.txn_info[txn]
        start_ts = info["start_ts"]
        writes = info["writes"]

        for item in writes:
            for ver in self.versions[item]:
                if ver["begin_ts"] > start_ts and ver["txn"] != txn:
                    self.aborted.add(txn)
                    self.active.discard(txn)
                    return {"result": "ABORT", "why": f"WW conflict on {item} (version at ts={ver['begin_ts']})"}

        commit_ts = self._next_ts()
        for item, val in writes.items():
            for ver in self.versions[item]:
                if ver["end_ts"] == float('inf'):
                    ver["end_ts"] = commit_ts
            self.versions[item].append({ "value": val, "begin_ts": commit_ts, "end_ts": float('inf'), "txn": txn })

        self.active.discard(txn)
        return {"result": "OK", "commit_ts": commit_ts}

    def abort(self, txn):
        self.aborted.add(txn)
        self.active.discard(txn)

    def get_state(self):
        state = {}
        for item in self.versions:
            for ver in reversed(self.versions[item]):
                if ver["end_ts"] == float('inf'):
                    state[item] = ver["value"]
                    break
        return state


class Runner:
    def __init__(self, engine, mode):
        self.engine = engine
        self.mode = mode
        self.trace = []
        self.step = 0
        self.aborted = set()
        self.pending = {}
        self.deferred = defaultdict(list)

    def _add_trace(self, **kwargs):
        self.step += 1
        entry = {"step": self.step}
        entry.update(kwargs)
        self.trace.append(entry)

    def run(self, events):

        idx = 0

        while idx < len(events):
            ev = events[idx]
            txn = ev["t"]
            op = ev["op"]

            if txn in self.aborted:
                idx += 1
                continue

            if txn in self.pending:
                self.deferred[txn].append(ev)
                idx += 1
                continue

            result = self._process_event(ev)

            if result and result.get("result") == "BLOCKED" and self.mode == "2pl":
                self.pending[txn] = ev
                self._add_trace(event="OP", t=txn, op=op, item=ev.get("item"), result="BLOCKED", why=result.get("why", ""))

                cycle = self.engine.detect_deadlock()
                if cycle:
                    victim = max(cycle[:-1])
                    self._add_trace(event="DEADLOCK", cycle=cycle, victim=victim)
                    self._do_abort(victim)
                    self._process_deferred()

            idx += 1

        self._process_deferred()
        self._resolve_pending()

    def _process_deferred(self):
        changed = True
        while changed:
            changed = False
            for txn in list(self.deferred.keys()):
                if txn in self.aborted:
                    del self.deferred[txn]
                    changed = True
                    continue
                if txn in self.pending:
                    continue

                while self.deferred[txn]:
                    ev = self.deferred[txn].pop(0)
                    if txn in self.aborted:
                        break

                    result = self._process_event(ev)

                    if result and result.get("result") == "BLOCKED" and self.mode == "2pl":
                        self.pending[txn] = ev
                        self._add_trace(event="OP", t=txn, op=ev["op"],
                                       item=ev.get("item"), result="BLOCKED",
                                       why=result.get("why", ""))
                        cycle = self.engine.detect_deadlock()
                        if cycle:
                            victim = max(cycle[:-1])
                            self._add_trace(event="DEADLOCK", cycle=cycle, victim=victim)
                            self._do_abort(victim)
                        break

                if not self.deferred.get(txn):
                    self.deferred.pop(txn, None)
                changed = True

    def _process_event(self, ev):
        txn = ev["t"]
        op = ev["op"]

        if op == "BEGIN":
            res = self.engine.begin(txn)
            self._add_trace(event="OP", t=txn, op="BEGIN", result="OK")
            return res

        elif op == "R":
            item = ev["item"]
            res = self.engine.read(txn, item)
            if res["result"] == "OK":
                self._add_trace(event="OP", t=txn, op="R", item=item,
                               result="OK", value=res["value"])
            return res

        elif op == "W":
            item = ev["item"]
            val = ev["value"]
            res = self.engine.write(txn, item, val)
            if res["result"] == "OK":
                self._add_trace(event="OP", t=txn, op="W", item=item,
                               value=val, result="OK")
            return res

        elif op == "COMMIT":
            if self.mode == "2pl":
                unblocked = self.engine.commit(txn)
                self._add_trace(event="OP", t=txn, op="COMMIT", result="OK")
                self._handle_unblocked(unblocked)
            else:
                res = self.engine.commit(txn)
                if res["result"] == "ABORT":
                    self._add_trace(event="OP", t=txn, op="COMMIT", result="ABORT", why=res["why"])
                    self._do_abort(txn)
                else:
                    self._add_trace(event="OP", t=txn, op="COMMIT", result="OK", commit_ts=res.get("commit_ts"))
            return None

        elif op == "ABORT":
            self._do_abort(txn)
            return None

    def _do_abort(self, txn):
        self.aborted.add(txn)
        self.pending.pop(txn, None)
        
        if self.mode == "2pl":
            unblocked = self.engine.abort(txn)
            self._add_trace(event="ABORT", t=txn)
            self._handle_unblocked(unblocked)
        else:
            self.engine.abort(txn)
            self._add_trace(event="ABORT", t=txn)

    def _handle_unblocked(self, unblocked):
        
        for (utxn, item, mode) in unblocked:
            if utxn in self.aborted:
                continue
            self._add_trace(event="UNBLOCK", t=utxn, item=item, lock_mode=mode)

            if utxn in self.pending:
                pev = self.pending.pop(utxn)
                res = self._process_event(pev)
                if res and res.get("result") == "BLOCKED":
                    self.pending[utxn] = pev

        self._process_deferred()

    def _resolve_pending(self):

        changed = True
        while changed and self.pending:
            changed = False
            for txn in list(self.pending.keys()):
                if txn in self.aborted:
                    self.pending.pop(txn)
                    changed = True
                    continue
                pev = self.pending[txn]
                res = self._process_event(pev)
                if res is None or res.get("result") == "OK":
                    self.pending.pop(txn)
                    changed = True

    def get_trace(self):
        return self.trace

    def get_final_state(self):
        return self.engine.get_state()


def main():
    parser = argparse.ArgumentParser(description="Conc control Sim")
    parser.add_argument("--cc", required=True, choices=["2pl", "mvcc"],
                        help="Conc control mode")
    
    parser.add_argument("--schedule", required=True, help="Path to schedule jsonl file")
    
    parser.add_argument("--out", required=True, help="Output dir")
    
    args = parser.parse_args()

    events = []
    with open(args.schedule) as f:
        for line in f:
            line = line.strip()
            if line:
                events.append(json.loads(line))

    if args.cc == "2pl":
        engine = TwoPLEngine(INITIAL_DB)
    else:
        engine = MVCCEngine(INITIAL_DB)
    #choose write one
    runner = Runner(engine, args.cc)
    runner.run(events)

    os.makedirs(args.out, exist_ok=True)

    with open(os.path.join(args.out, "trace.jsonl"), "w") as f:
        for entry in runner.get_trace():
            f.write(json.dumps(entry) + "\n")

    with open(os.path.join(args.out, "final_state.json"), "w") as f:
        json.dump(runner.get_final_state(), f, indent=2)

    print(f"Mode: {args.cc.upper()}")
    print(f"Final state: {runner.get_final_state()}")
    print(f"Trace: {len(runner.get_trace())} steps written {args.out}/trace.jsonl")


if __name__ == "__main__":
    main()
