/**
 * @file pubsub.hpp
 * @brief Header‑only, lock‑free publish/subscribe messaging framework.
 *
 * The framework is designed for low(ish)‑latency intra‑process
 * communication between many *publishers* and *subscribers* running in
 * different threads.  It relies on a small, fixed allocation pool for
 * messages and uses Boost.Lockfree queues to avoid the overhead and
 * contention associated with standard thread‑safe queues.
 *
 *   * **MessageContainer** – type‑erased storage for the concrete payload
 *     types supplied by the user.
 *   * **SharedMessage**      – thin intrusive smart‑pointer that reference
 *     counts MessageContainers.
 *   * **MessageQueue**       – MPMC queue with a tiny locked overflow path
 *     for back‑pressure handling.
 *   * **Topic**              – hierarchical topic tree ("a.b.c") that fans
 *     out messages to an arbitrary number of queues.
 *   * **PublisherSubscriber** – base class that glues the above together
 *     and gives the application a convenient interface.
 *
 * The library intentionally keeps memory ownership explicit: the
 * application owns the *Pool*, pulls a fresh *SharedMessage*, fills it
 * with a payload, and publishes it.  Subscribers receive another
 * *SharedMessage*, guaranteeing the payload remains alive until every
 * consumer releases its reference.
 *
 * The code is header‑only and freestanding except for
 * `Boost.Lockfree` and (optionally) the `quill` logging library.
 *
 * @note All public types live inside the `pubsub` namespace.
 */

#pragma once

#include <boost/lockfree/queue.hpp>

#include <atomic>
#include <memory>
#include <mutex>
#include <optional>
#include <queue>
#include <thread>
#include <unordered_map>
#include <variant>
#include <vector>

/**
 * ---------------------------------------------------------------------------
 * Optional logging façade
 * ---------------------------------------------------------------------------
 * Unless the build defines `PUBSUB_USE_QUILL` the log‑macros expand to no‑ops,
 * keeping the entire logging path out of the binary.
 */
#ifdef PUBSUB_USE_QUILL
#include <quill/Backend.h>
#include <quill/Frontend.h>
#include <quill/LogMacros.h>
#include <quill/Logger.h>
#include <quill/sinks/ConsoleSink.h>
#include <quill/sinks/FileSink.h>

#define PUBSUB_LOG_INFO(...) LOG_INFO(__VA_ARGS__)
#define PUBSUB_LOG_DEBUG(...) LOG_DEBUG(__VA_ARGS__)
#define PUBSUB_LOG_WARNING(...) LOG_WARNING(__VA_ARGS__)
#define PUBSUB_LOG_ERROR(...) LOG_ERROR(__VA_ARGS__)
#define PUBSUB_LOG_CRITICAL(...) LOG_CRITICAL(__VA_ARGS__)

namespace pubsub::logging {
using level = quill::LogLevel; ///< Logging level alias.
using Logger =
    quill::Logger*; ///< Thin pointer alias used throughout the framework.

/**
 * @brief Create (or fetch) a console backed logger.
 * @param name Unique name – logger instances are cached by the backend.
 */
inline Logger create_logger(const std::string& name) {
    return quill::Frontend::create_or_get_logger(
        name, quill::Frontend::create_or_get_sink<quill::ConsoleSink>(
                  "default_sink"));
}

/// Start the dedicated Quill backend thread.
inline void start_backend() { quill::Backend::start(); }

/// Set a runtime log level on a logger returned by @ref create_logger.
inline void set_log_level(Logger logger, level log_level) {
    logger->set_log_level(log_level);
}
} // namespace pubsub::logging
#else
// Log‑macros collapse to nothing.
#define PUBSUB_LOG_INFO(...)
#define PUBSUB_LOG_DEBUG(...)
#define PUBSUB_LOG_WARNING(...)
#define PUBSUB_LOG_ERROR(...)
#define PUBSUB_LOG_CRITICAL(...)

namespace pubsub::logging {
/**
 * @brief Minimal stub enumerating the log levels used in the code base.
 *        Present to avoid `#ifdef` spaghetti in the rest of the headers.
 */
enum class level { Info, Debug, Warning, Error, Critical };
using Logger = void*; ///< Dummy alias – pointers are never dereferenced.
inline Logger create_logger(const std::string&) { return nullptr; }
inline void start_backend() {}
inline void set_log_level(Logger logger, level log_level) {}
} // namespace pubsub::logging
#endif // PUBSUB_USE_QUILL

namespace pubsub {

static inline constexpr size_t cache_line_size = 64U;

// ==========================================================================
// Utility: TopicType
// ==========================================================================

/**
 * @brief Convenience helper representing a hierarchical topic such as
 *        {"trades", "BTC", "fills"}.
 *
 *  * Internally inherits from `std::vector<std::string>` so the usual
 *    container operations – `push_back`, iterators, `size()` – work as
 *    expected.
 *  * The helper @ref to_string converts the hierarchy into the canonical
 *    dotted form ("trades.BTC.fills").
 */
class TopicType : public std::vector<std::string> {
  public:
    using std::vector<std::string>::vector; ///< Inherit all base ctors.

    /// @return Topic as dot‑separated string without a trailing dot.
    std::string to_string() const {
        std::string result;
        for (const auto& str : *this) {
            result += str + ".";
        }
        if (!result.empty()) {
            result.pop_back(); // Remove the trailing dot
        }
        return result;
    }
};

// ==========================================================================
// Utility: SharedMessage – intrusive smart pointer
// ==========================================================================

/**
 * @tparam MessageType The intrusive type that implements `incref()`/
 *         `decref()`; typically @ref MessageContainer.
 *
 * A *zero‑overhead* smart pointer that forwards reference management to
 * the underlying message object.  Copying / moving follows the expected
 * pointer semantics – copies bump the ref‑count, moves transfer
 * ownership.
 */
template <typename MessageType> class SharedMessage {
  private:
    MessageType* m_message = nullptr;

  public:
    /// Acquire a reference to @p message (may be nullptr).
    explicit SharedMessage(MessageType* message) : m_message(message) {
        if (m_message) {
            m_message->incref();
        }
    }

    /// Release reference on destruction.
    ~SharedMessage() {
        if (m_message) {
            m_message->decref();
        }
    }

    /// Copy: increments reference count.
    SharedMessage(const SharedMessage& other) : m_message(other.m_message) {
        if (m_message) {
            m_message->incref();
        }
    }

    /// Copy: increments reference count.
    SharedMessage& operator=(const SharedMessage& other) {
        if (this != &other) {
            if (m_message) {
                m_message->decref();
            }
            m_message = other.m_message;
            if (m_message) {
                m_message->incref();
            }
        }
        return *this;
    }

    /// Move constructor – transfers pointer without touching counters.
    SharedMessage(SharedMessage&& other) noexcept : m_message(other.m_message) {
        other.m_message = nullptr;
    }

    /// Move assignment.
    SharedMessage& operator=(SharedMessage&& other) noexcept {
        if (this != &other) {
            if (m_message) {
                m_message->decref();
            }
            m_message = other.m_message;
            other.m_message = nullptr;
        }
        return *this;
    }

    // -------------------------------------------------------------------
    // Raw access helpers – identical to std::shared_ptr<T> interface.
    // -------------------------------------------------------------------
    MessageType* get() { return m_message; }
    const MessageType* get() const { return m_message; }

    MessageType* operator->() { return m_message; }
    const MessageType* operator->() const { return m_message; }

    MessageType& operator*() { return *m_message; }
    const MessageType& operator*() const { return *m_message; }
};

// ==========================================================================
// Core: MessageContainer / Pool
// ==========================================================================

/**
 * @brief Type‑erased message payload holder with intrusive ref‑counting.
 *
 * `MessageContainer` stores one value of *exactly* one of the types from
 * `MessageTypes...`.  The concrete variant is set via @ref set_data and
 * later visited through @ref visit.  Payload objects live **in‑place** –
 * no additional heap indirection.
 *
 * To eliminate dynamic allocations at run‑time, containers are recycled
 * by an embedded @ref Pool.  A message returns to its pool as soon as
 * *all* SharedMessage references are gone.
 */
template <typename... MessageTypes> class MessageContainer {
  public:
    class Pool; ///< Forward decl for intrusive ptr.
    using self_t = MessageContainer<MessageTypes...>;

  private:
    std::variant<MessageTypes...> m_data; ///< Actual payload.
    Pool* m_pool{nullptr};                ///< Owning pool.
    TopicType* m_topic_string{nullptr};   ///< Topic string (if any).
    TopicType m_empty_topic_string;       ///< Fallback when not set.
    logging::Logger m_logger;
    alignas(cache_line_size) std::atomic<int64_t> m_ref_cnt{
        0}; ///< Intrusive counter.

  public:
    MessageContainer(Pool* pool)
        : m_pool(pool), m_logger(logging::create_logger("message-container")) {}

    // -------------------------------------------------------------------
    // Pool: fixed‑size allocation / recycling of MessageContainers
    // -------------------------------------------------------------------

    /**
     * @brief Bump‑the‑pointer object pool.
     *
     *   * The pool holds a vector of `unique_ptr< MessageContainer >` to
     *     own the actual memory.
     *   * A separate lock‑free free‑list (MPSC) hands out pointers to
     *     available messages.
     *   * In the rare case the free‑list is empty the pool *expands*
     *     under a mutex.
     */
    class Pool {
      private:
        const size_t m_free_queue_size;                         ///< Capacity.
        boost::lockfree::queue<MessageContainer*> m_free_queue; ///< Free list.
        std::mutex m_mutex; ///< Mutex for expansion of stored messages.
        std::vector<std::unique_ptr<MessageContainer>>
            m_messages; ///< Stored messages.

        /// Return a container to the free list (called by `decref`).
        void release(MessageContainer* msg) {
            if (msg->in_use()) {
                throw std::runtime_error("Message is still in use");
            }
            PUBSUB_LOG_DEBUG(msg->m_logger, "releasing message");
            if (!m_free_queue.push(msg)) {
                PUBSUB_LOG_ERROR(msg->m_logger, "free queue is full");
            }
        }

      public:
        /**
         * @param initial_size     Number of MessageContainers pre‑allocated.
         * @param free_queue_size  Capacity of the lock‑free free list.
         */
        Pool(size_t initial_size = 1024, size_t free_queue_size = 2048)
            : m_free_queue_size(free_queue_size),
              m_free_queue(m_free_queue_size) {
            m_messages.reserve(initial_size);
            for (size_t i = 0; i < initial_size; ++i) {
                m_free_queue.push(
                    m_messages
                        .emplace_back(std::make_unique<MessageContainer>(this))
                        .get());
            }
        }

        /**
         * @brief Acquire a fresh, *exclusive* SharedMessage from the pool.
         *
         * If the free list is exhausted – which should only happen under
         * sustained congestion – the pool expands under a mutex.
         */
        SharedMessage<self_t> get() {
            MessageContainer* msg = nullptr;
            if (m_free_queue.pop(msg)) {
                if (msg->in_use()) {
                    throw std::runtime_error("Message is already in use");
                }
            } else {
                std::lock_guard<std::mutex> lock(m_mutex);
                msg =
                    m_messages
                        .emplace_back(std::make_unique<MessageContainer>(this))
                        .get();
            }
            return SharedMessage<self_t>(msg);
        }

        friend MessageContainer; // Needed for `release` call.
    };

    // -------------------------------------------------------------------
    // Intrusive reference counting helpers – used by SharedMessage
    // -------------------------------------------------------------------

    /** @brief Increment reference count (relaxed order – no synchronisation).
     */
    void incref() { m_ref_cnt.fetch_add(1, std::memory_order_relaxed); }

    /** @brief Decrement reference count and recycle if we hit zero. */
    void decref() {
        if (m_ref_cnt.fetch_sub(1, std::memory_order_relaxed) == 1) {
            m_topic_string = nullptr;
            m_pool->release(this);
        }
    }

    /// @return Current reference count (debug / asserts only).
    int64_t ref_count() const {
        return m_ref_cnt.load(std::memory_order_relaxed);
    }

    /// @return `true` if at least one SharedMessage still references us.
    bool in_use() const { return ref_count() > 0; }

    // -------------------------------------------------------------------
    // Payload / meta‑data setters
    // -------------------------------------------------------------------

    /** @brief Point the container to the *owning* Topic’s path string. */
    void set_topic_string(TopicType* topic_string) {
        m_topic_string = topic_string;
    }

    /** @brief Set arbitrary payload in the container. */
    template <typename T> void set_data(const T& data) { m_data = data; }

    /** @brief Perfect‑forward arbitrary payload into the container. */
    template <typename T> void set_data(T&& data) {
        m_data = std::forward<T>(data);
    }

    // -------------------------------------------------------------------
    // Visitor – deliver payload together with topic.
    // -------------------------------------------------------------------

    /**
     * @tparam Visitor Functor with signature
     *                 `void(const TopicType&, const Concrete&)`.
     * @param visitor  Will be invoked exactly once with the stored variant.
     */
    template <typename Visitor> void visit(Visitor&& visitor) {
        Visitor& loaded = visitor;
        visit(loaded);
    }

    /**
     * @tparam Visitor Functor with signature
     *                 `void(const TopicType&, const Concrete&)`.
     * @param visitor  Will be invoked exactly once with the stored variant.
     */
    template <typename Visitor> void visit(Visitor& visitor) {
        const auto& topic =
            (m_topic_string ? *m_topic_string : m_empty_topic_string);
        std::visit([&](const auto& message) { visitor(topic, message); },
                   m_data);
    }
};

// ==========================================================================
// Configuration helpers – tailor queues / topics to a message set
// ==========================================================================

template <typename... MessageTypes> struct MessageConfig {
    using RawMessage = MessageContainer<MessageTypes...>; ///< Intrusive type.
    using Pool = typename RawMessage::Pool;               ///< Pool alias.
    using Message = SharedMessage<RawMessage>; ///< User‑facing handle.

    static constexpr size_t fast_queue_size = 1024; ///< Ring capacity.
    static constexpr size_t max_subscribers = 32;   ///< Per‑topic limit.
};

// ==========================================================================
// MessageQueue – lock‑free fast path + locked overflow path
// ==========================================================================

/**
 * @brief Multi‑producer / single‑consumer queue optimised for the
 *        publisher‑subscriber pattern.
 *
 * A bounded lock‑free ring (Boost.Lockfree) constitutes the *fast path*.
 * If the ring is full, writers push to a secondary std::queue protected
 * by a mutex.  The consumer drains the slow path lazily before popping
 * from the ring.  This gives us predictable latency under normal load
 * yet avoids message loss during rare bursts.
 *
 * @tparam config MessageConfig specialisation used throughout the tree.
 */
template <typename config> class MessageQueue {
  public:
    using Message = typename config::Message;
    using RawMessage = typename config::RawMessage;

  private:
    // Slow overflow path
    std::queue<RawMessage*> m_slow_queue;
    std::mutex m_mutex;
    std::atomic<size_t> m_slow_size{0};

    // Fast lock‑free ring
    boost::lockfree::queue<RawMessage*,
                           boost::lockfree::capacity<config::fast_queue_size>>
        m_fast_queue;

    logging::Logger m_logger;

    // Push into mutex‑protected queue when ring is full.
    void push_slow(RawMessage* msg) {
        std::lock_guard<std::mutex> lock(m_mutex);
        m_slow_queue.push(msg);
        m_slow_size.fetch_add(1, std::memory_order_release);
        PUBSUB_LOG_WARNING(m_logger, "pushed to slow queue: slow queue size={}",
                           m_slow_size.load(std::memory_order_relaxed));
    }

    // Drain slow queue back to ring. Consumer side only.
    void drain_slow() {
        if (m_slow_size.load(std::memory_order_acquire) > 0) {
            std::lock_guard<std::mutex> lock(m_mutex);
            while (!m_slow_queue.empty()) {
                RawMessage* msg = m_slow_queue.front();
                if (!m_fast_queue.push(msg)) {
                    break;
                }
                m_slow_queue.pop();
            }
            m_slow_size.store(m_slow_queue.size(), std::memory_order_release);
        }
    }

  public:
    MessageQueue() : m_logger(logging::create_logger("message-queue")) {}

    ~MessageQueue() {
        // Return *all* messages back to the pool.
        RawMessage* msg = nullptr;
        while (m_fast_queue.pop(msg)) {
            msg->decref();
        }
        while (m_slow_queue.size() > 0) {
            msg = m_slow_queue.front();
            m_slow_queue.pop();
            msg->decref();
        }
    }

    /** @brief Non‑blocking push usable from *any* thread. */
    void push(Message msg) {
        RawMessage* raw_msg = msg.get();
        raw_msg->incref(); // Consumer will own another ref.
        if (!m_fast_queue.push(raw_msg)) {
            push_slow(raw_msg);
        }
    }

    /**
     * @return Next message or `std::nullopt` when the queue is empty.
     */
    std::optional<Message> pop() {
        drain_slow(); // Give overflow messages a chance first.

        RawMessage* msg = nullptr;
        if (m_fast_queue.pop(msg)) {
            auto out = Message(msg); // Acquire consumer reference.
            msg->decref();           // Drop queue’s own reference
            return std::move(out);
        }

        return std::nullopt;
    }
};

// ==========================================================================
// LockFreeVector – append‑only array for subscriber handles
// ==========================================================================

/**
 * @brief Lock‑free, append‑only vector with a fixed upper bound.
 *
 * Used by @ref Topic to store subscriber queues without a mutex on the
 * hot publish path.  Insertions are linearizable and wait‑free (up to
 * `max_size`).  The vector *never shrinks*.
 */
template <typename T> class LockFreeVector {
  private:
    std::vector<T> m_data;
    alignas(cache_line_size) std::atomic<size_t> m_next_index{
        0}; ///< Index of next free slot.
    alignas(cache_line_size) std::atomic<size_t> m_size{
        0}; ///< Number of *initialized* slots.

    size_t get_free_index() {
        size_t index = m_next_index.fetch_add(1, std::memory_order_acq_rel);
        if (index >= m_data.size()) {
            throw std::runtime_error("Index out of bounds");
        }
        return index;
    }

    // Publish new size monotonically.
    void update_size(size_t index) {
        size_t index_swap = index;
        while (!m_size.compare_exchange_weak(index_swap, index + 1,
                                             std::memory_order_release,
                                             std::memory_order_relaxed)) {
            if (index_swap > index) {
                throw std::runtime_error("Index swap is greater than index");
            }
            index_swap = index;
        }
    }

  public:
    LockFreeVector(size_t max_size) : m_data(max_size) {}

    // append
    size_t push_back(const T& value) {
        size_t index = get_free_index();
        m_data[index] = value;
        update_size(index);
        return index;
    }
    size_t push_back(T&& value) {
        size_t index = get_free_index();
        m_data[index] = std::forward<T>(value);
        update_size(index);
        return index;
    }
    template <typename... Args> size_t emplace_back(Args&&... args) {
        size_t index = get_free_index();
        std::construct_at(&m_data[index], std::forward<Args>(args)...);
        update_size(index);
        return index;
    }

    // size / iteration
    size_t size() const { return m_size.load(std::memory_order_relaxed); }

    T& operator[](size_t index) { return m_data[index]; }
    const T& operator[](size_t index) const { return m_data[index]; }

    T* begin() { return m_data.data(); }
    T* end() { return m_data.data() + size(); }

    const T* begin() const { return m_data.data(); }
    const T* end() const { return m_data.data() + size(); }
};

// ==========================================================================
// Topic – hierarchical fan‑out tree
// ==========================================================================

/**
 * @brief Node inside a hierarchical topic tree.
 *
 * Each node owns a `LockFreeVector` of subscriber queues that receive a
 * *copy* (reference) of every message published to that node.
 *
 * Sub‑topics are created lazily on first access.  Subscription is *deep*:
 * when a queue subscribes to a parent topic it will automatically be
 * registered to all current and future sub‑topics.
 */
template <typename config> class Topic {
  public:
    using self_t = Topic<config>;

  private:
    using Message = typename config::Message;
    using Queue = MessageQueue<config>;

    const std::string m_name; ///< Local component.
    LockFreeVector<std::shared_ptr<Queue>> m_subscribers{
        config::max_subscribers};

    std::unordered_map<std::string, std::unique_ptr<self_t>> m_sub_topics;
    std::mutex m_mutex; ///< Protects map mutations.

    TopicType m_topic_string; ///< Cached full path.

    // Internal helper – assumes mutex held.
    self_t* create_sub_topic_no_lock(const std::string& name) {
        auto& sub_topic = (m_sub_topics[name] =
                               std::make_unique<self_t>(name, m_topic_string));
        for (auto& queue : m_subscribers) {
            sub_topic->add_subscriber(queue);
        }
        return sub_topic.get();
    }

  public:
    /**
     * @param name         Local topic component ("fills"). Empty for root.
     * @param topic_string Full path up to the *parent* (root default).
     */
    explicit Topic(const std::string& name, TopicType topic_string = {})
        : m_name(name), m_topic_string(topic_string) {
        if (!(name.empty() && topic_string.empty())) {
            m_topic_string.push_back(name);
        }
    }

    /**
     * @brief Traverse / create sub‑topics according to @p topic_string.
     * @return Pointer to the leaf node matching the full path.
     */
    self_t* find(TopicType topic_string) {
        if (topic_string.size() == 0) {
            return this;
        }
        auto* next = get_or_create_sub_topic(topic_string[0]);
        topic_string.erase(topic_string.begin());
        return next->find(topic_string);
    }

    /// Thread‑safe creation / lookup of a direct child.
    self_t* get_or_create_sub_topic(const std::string& name) {
        std::lock_guard<std::mutex> lock(m_mutex);
        auto it = m_sub_topics.find(name);
        if (it != m_sub_topics.end()) {
            return it->second.get();
        }
        return create_sub_topic_no_lock(name);
    }

    /**
     * @brief Attach @p queue to this topic *and* recursively to all
     *        existing sub‑topics.
     */
    void add_subscriber(std::shared_ptr<Queue> queue) {
        m_subscribers.push_back(queue);
        std::lock_guard<std::mutex> lock(m_mutex);
        for (auto& [name, sub_topic] : m_sub_topics) {
            sub_topic->add_subscriber(queue);
        }
    }

    /** @brief Fan‑out a message reference to every current subscriber. */
    void publish(Message msg) {
        msg->set_topic_string(&m_topic_string);
        for (auto& queue : m_subscribers) {
            if (queue) {
                queue->push(msg);
            }
        }
    }
};

/**
 * @brief Singleton accessor for the root topic of a given configuration.
 */
template <typename config>
inline std::shared_ptr<Topic<config>> get_top_level_topic() {
    static std::shared_ptr<Topic<config>> top_level_topic =
        std::make_shared<Topic<config>>("");
    return top_level_topic;
}

// ==========================================================================
// Base interface used by Thread launcher / runner code
// ==========================================================================

class PublisherSubscriberBase {
  public:
    using TopicKey = uint64_t; ///< Opaque handle returned by get_topic_key().
    virtual ~PublisherSubscriberBase() = default;
    virtual void launch() = 0; ///< Must start internal processing thread.
};

// ==========================================================================
// CRTP convenience wrapper – derive and override on_message/on_* hooks
// ==========================================================================

/**
 * @tparam config  MessageConfig specialisation describing payload set.
 * @tparam Derived CRTP type implementing the callback hooks.
 *
 * The class hides almost all boilerplate involved in setting‑up a
 * publish/subscribe participant.  The user derives from the template and
 * implements:
 *
 *   * `void on_message(const TopicType& topic, const Payload& data)` –
 *     invoked for every inbound message.
 *   * `void on_launch()`  – optional, called once inside worker thread.
 *   * `void on_close()`   – optional, called once before thread exits.
 *   * `void on_process()` – optional, called *each loop* after all
 *     pending messages have been processed.
 */
template <typename config, typename Derived>
class PublisherSubscriber : public PublisherSubscriberBase {
  private:
    using Pool = typename config::Pool;
    using Queue = MessageQueue<config>;

    Pool m_pool;              ///< Intrusive message pool.
    const std::string m_name; ///< Human readable name.
    std::shared_ptr<Topic<config>> m_top_level_topic; ///< Root of topic tree.
    std::shared_ptr<Queue> m_queue; ///< Private inbound queue.
    logging::Logger m_logger;       ///< Thread‑safe logger.

    std::atomic<bool> m_should_run = false;
    std::thread m_thread; ///< Worker thread.

    std::vector<Topic<config>*>
        m_publish_topics; ///< Cached pointers for fast publish.

    // Cast helpers
    Derived* derived() { return static_cast<Derived*>(this); }
    const Derived* derived() const { return static_cast<const Derived*>(this); }

  public:
    explicit PublisherSubscriber(const std::string& name = "unnamed")
        : m_name(name), m_top_level_topic(get_top_level_topic<config>()),
          m_queue(std::make_shared<Queue>()),
          m_logger(
              logging::create_logger(m_name + "-" + std::to_string(id()))) {}

    // -------------------------------------------------------------------
    // Destructor – clean shutdown of worker thread.
    // -------------------------------------------------------------------
    virtual ~PublisherSubscriber() {
        m_should_run.store(false, std::memory_order_relaxed);
        if (m_thread.joinable()) {
            m_thread.join();
        }
    }

    // -------------------------------------------------------------------
    // Meta data / convenience getters
    // -------------------------------------------------------------------
    const std::string& name() const { return m_name; }
    uint64_t id() const { return (uint64_t) static_cast<const void*>(this); }

    logging::Logger logger() { return m_logger; }
    const logging::Logger logger() const { return m_logger; }

    // -------------------------------------------------------------------
    // Hooks – intended to be overridden by Derived
    // -------------------------------------------------------------------
    virtual void on_launch() {}
    virtual void on_close() {}
    virtual void on_process() {}

    // -------------------------------------------------------------------
    // Subscription / publication API
    // -------------------------------------------------------------------

    /**
     * @brief Subscribe the instance’s private queue to @p topic.
     * @note May be called from *any* thread before or after `launch()`.
     */
    void subscribe(TopicType topic) {
        m_top_level_topic->find(topic)->add_subscriber(m_queue);
    }

    /**
     * @brief Register a publication topic for fast repeated access.
     * @return Opaque key usable in subsequent publish() calls.
     */
    TopicKey get_topic_key(TopicType topic) {
        auto* sub_topic = m_top_level_topic->find(topic);
        auto key = m_publish_topics.size();
        m_publish_topics.push_back(sub_topic);
        return key;
    }

    /// Publish *r‑value* payload.
    template <typename T> void publish(TopicKey key, T&& data) {
        auto msg = m_pool.get();
        msg->set_data(std::forward<T>(data));
        m_publish_topics[key]->publish(std::move(msg));
    }

    /// Publish *l‑value* payload (copy).
    template <typename T> void publish(TopicKey key, const T& data) {
        auto msg = m_pool.get();
        msg->set_data(data);
        m_publish_topics[key]->publish(std::move(msg));
    }

    // -------------------------------------------------------------------
    // Main processing method – runs in a loop inside dedicated thread
    // -------------------------------------------------------------------
    void process() {
        auto msg = m_queue->pop();
        if (msg) {
            (*msg)->visit([this](const auto& topic, const auto& data) {
                derived()->on_message(topic, data);
            });
        }
        derived()->on_process();
    }

    /**
     * @brief Spawn worker thread and enter message loop.
     *        The thread runs until the object is destroyed.
     */
    void launch() override {
        m_should_run = true;
        m_thread = std::thread([&]() {
            this->on_launch();
            while (m_should_run.load(std::memory_order_relaxed)) {
                this->process();
            }
            this->on_close();
        });
    }
};

} // namespace pubsub