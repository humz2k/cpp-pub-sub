/**
 * @file pingpong_example.cpp
 * @brief Minimal end‑to‑end demonstration of the pubsub framework.
 *
 * The example starts three PublisherSubscriber participants:
 *
 *   1. **PingPong ping0** — publishes integers on topic `topic0.subtopic0`
 *      and listens on `topic0.subtopic1`.
 *   2. **PingPong ping1** — mirror image of `ping0`.
 *   3. **Listener**       — listens on the **root** topic and therefore
 *      receives every message published anywhere in the tree.
 *
 * The two PingPong instances bounce an integer counter back and forth
 * until the value reaches @p n_pings.  Each of them additionally emits
 * a one‑off *greeting* string message to demonstrate heterogeneous
 * payload support.
 */

#include <pubsub/pubsub.hpp>

#include <chrono>
#include <thread>

// Convenience alias for a config that supports one payload type:
//   * size_t        (the ping counter)
using config = pubsub::MessageConfig<size_t>;

/**
 * @class PingPong
 * @brief Simple producer/consumer that bounces a counter between two topics.
 *
 * Diagram (→ publish, ← subscribe):
 * @verbatim
 *     ping0   → topic0.subtopic0         topic0.subtopic1 ←   ping1
 *              ↑                                                   ↓
 *              |                                                   |
 *              └────────────────── listener ───────────────────────┘
 * @endverbatim
 *
 * The first instance (constructed with @c initiator==true) seeds the
 * interaction by publishing the integer `0`.  Upon receiving a value
 * each side increments it and republishes to the opposite topic until
 * the counter exceeds @p m_n_pings.
 *
 * A one‑time greeting string is published from @ref on_launch to show
 * that a single topic can carry heterogeneous payloads.
 */
class PingPong : public pubsub::PublisherSubscriber<config, PingPong> {
  private:
    TopicKey m_send_topic; ///< Cached handle for the publication topic.
    bool m_initiator;      ///< Starts the game when @c true.

  public:
    /**
     * @param send_topic Topic on which *this* instance publishes counters.
     * @param recv_topic Topic it subscribes to.
     * @param n_pings    Number of counter exchanges before stopping.
     * @param initiator  Whether this side sends the first “ping”.
     */
    PingPong(pubsub::TopicType send_topic, pubsub::TopicType recv_topic,
             bool initiator)
        : pubsub::PublisherSubscriber<config, PingPong>("pingpong"),
          m_send_topic(get_topic_key(send_topic)), m_initiator(initiator) {
        subscribe(recv_topic);
    }

    /// Called once inside the worker thread right after it starts.
    void on_launch() override {
        if (m_initiator) {
            publish<size_t>(m_send_topic, 0); // Kick things off.
        }
    }

    /// Handler for incoming *size_t* messages.
    void on_message(const pubsub::TopicType& topic, size_t data) {
        publish(m_send_topic, data + 1); // Bounce back.
    }

    /// Catch‑all overload for other payload types (ignored).
    template <typename T>
    void on_message(const pubsub::TopicType& topic, const T& data) {}
};

int main() {
    // Quill uses a dedicated backend thread. Start it once per process.
    pubsub::logging::start_backend();

    // Construct two complementary PingPong instances.
    PingPong ping0({"topic0", "subtopic0"}, {"topic0", "subtopic1"}, true);
    PingPong ping1({"topic0", "subtopic1"}, {"topic0", "subtopic0"}, false);

    ping0.launch();
    ping1.launch();

    pubsub::run_forever([] {});
    return 0;
}