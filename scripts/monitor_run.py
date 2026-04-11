import os, requests, time, json
from dotenv import load_dotenv
load_dotenv()
HOST = os.getenv("DATABRICKS_HOST","").rstrip("/")
TOKEN = os.getenv("DATABRICKS_TOKEN","")
H = {"Authorization": f"Bearer {TOKEN}"}

run_id = "688887728623506"

for i in range(60):  # 10 min
    time.sleep(10)
    st = requests.get(f"{HOST}/api/2.1/jobs/runs/get", headers=H, params={"run_id": run_id}).json()
    state = st.get("state", {})
    lcs = state.get("life_cycle_state", "?")
    
    parts = []
    for t in st.get("tasks", []):
        tk = t["task_key"]
        ts = t.get("state", {}).get("life_cycle_state", "?")
        parts.append(f"{tk}={ts}")
        if ts == "RUNNING":
            print(f"[{(i+1)*10}s] >>> CLUSTER STARTED! {tk} IS RUNNING! <<<")
            # Keep monitoring to completion
            for j in range(180):
                time.sleep(10)
                st2 = requests.get(f"{HOST}/api/2.1/jobs/runs/get", headers=H, params={"run_id": run_id}).json()
                state2 = st2.get("state", {})
                lcs2 = state2.get("life_cycle_state", "?")
                parts2 = []
                for t2 in st2.get("tasks", []):
                    parts2.append(f"{t2['task_key']}={t2.get('state',{}).get('life_cycle_state','?')}")
                et = (i+1)*10 + (j+1)*10
                print(f"[{et:5d}s] {lcs2} | {', '.join(parts2)}")
                if lcs2 in ("TERMINATED", "INTERNAL_ERROR"):
                    print(f"\nFINAL: {state2.get('result_state','?')}")
                    for t2 in st2.get("tasks", []):
                        s2 = t2.get("state", {})
                        print(f"  {t2['task_key']}: {s2.get('result_state','?')} - {s2.get('state_message','')[:200]}")
                    exit(0)
            exit(0)
    
    if lcs in ("TERMINATED", "INTERNAL_ERROR"):
        print(f"[{(i+1)*10}s] {lcs}: {state.get('result_state','?')}")
        for t in st.get("tasks", []):
            s = t.get("state", {})
            print(f"  {t['task_key']}: {s.get('result_state','?')} - {s.get('state_message','')[:200]}")
        break
    
    elapsed = (i+1)*10
    print(f"[{elapsed:4d}s] {lcs} | {', '.join(parts)}")

print("Done monitoring.")
