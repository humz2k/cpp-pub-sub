# cpp-pub-sub
> **Simple intra-process messaging for games, trading engines and real-time dashboards.**

---

## Highlights
* **Header-only** – just drop one header (`pubsub.hpp`) into your include path
* **Intrusive zero-copy messages** – payloads live *in place*; hand-off is a single pointer move
* **Bounded lock-free fast path** – Boost.Lockfree ring for predictable tail-latency
* **Overflow buffer** – tiny mutex-protected queue absorbs rare bursts without dropping data
* **Hierarchical topics** – `trades.BTC.fills` style routing with fan-out to N subscribers
* **CRTP convenience wrapper** – derive, override `on_message()`, you’re done
* Optional **Quill** integration for structured, zero-alloc logging