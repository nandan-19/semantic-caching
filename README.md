# FSCgRPC
Semantic Caching for gRPC requests

To run the project:

```
git clone 
```

## Cache admission

The gateway admits a nearest Redis-vector candidate with one of three modes.
The default keeps the existing Shannon threshold and deterministic semantic guard.

| `CACHE_DECIDER` | Admission path |
| --- | --- |
| `shannon` | Shannon threshold → deterministic semantic guard |
| `laya_verify` | Shannon threshold → deterministic semantic guard → Laya verifier |
| `laya` | Broad vector-distance ceiling → deterministic semantic guard → Laya verifier |

Laya modes call a separately hosted [Laya](https://github.com/NandhaKishorM/laya) service at `http://127.0.0.1:8000/v1/systemone` by default. Start Laya independently, then configure the gateway before starting it:

```powershell
$env:CACHE_DECIDER = "laya_verify"
$env:LAYA_URL = "http://127.0.0.1:8000/v1/systemone"
$env:LAYA_MIN_CONFIDENCE = "0.85"
```

`laya_verify` keeps Shannon as the low-cost shortlist and uses Laya only for candidates Shannon would accept. `laya` lets Laya make the admission decision while `LAYA_MAX_DISTANCE` (default `0.60`) prevents clearly unrelated vector matches from reaching the verifier. A Laya error or low-confidence answer rejects the semantic hit and continues to the fallback path.
