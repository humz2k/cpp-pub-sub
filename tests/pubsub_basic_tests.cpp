#include <gtest/gtest.h>

#include <pubsub/pubsub.hpp>

TEST(pubsub_message_tests, basic0) {
    using config = pubsub::MessageConfig<int>;
    config::Pool pool;

    auto message = pool.get();
    message->set_data(5);
    message->visit([&](const auto& topic, const auto& value) {
        EXPECT_EQ(value, 5);
        EXPECT_EQ(topic.size(), 0);
    });
}

TEST(pubsub_message_tests, basic1) {
    using config = pubsub::MessageConfig<int, double>;
    config::Pool pool;

    auto message1 = pool.get();
    message1->set_data(5);
    message1->visit([&](const auto& topic, const auto& value) {
        EXPECT_EQ(value, 5);
        EXPECT_EQ(topic.size(), 0);
    });

    pubsub::TopicType topic_string({"my", "topic", "string"});
    auto message2 = pool.get();
    message2->set_topic_string(&topic_string);
    message2->set_data(5.5);
    message2->visit([&](const auto& topic, const auto& value) {
        EXPECT_EQ(value, 5.5);
        EXPECT_EQ(topic, topic_string);
    });
}

TEST(pubsub_message_tests, refcount) {
    using config = pubsub::MessageConfig<int>;
    config::Pool pool;

    auto m0 = pool.get(); // ref = 1 (pool → test handle)
    EXPECT_EQ(m0->ref_count(), 1);

    {
        auto copy = m0; // +1
        EXPECT_EQ(m0->ref_count(), 2);
    } // copy destroyed → −1
    EXPECT_EQ(m0->ref_count(), 1);

    auto moved = std::move(m0); // transfer without inc/dec
    EXPECT_EQ(moved->ref_count(), 1);
}

TEST(pubsub_queue_tests, push_pop_fast_path) {
    using config = pubsub::MessageConfig<int>;
    config::Pool pool;
    pubsub::MessageQueue<config> q;

    auto msg = pool.get();
    msg->set_data(42);

    q.push(msg);
    auto popped = q.pop();
    ASSERT_TRUE(popped);
    (*popped)->visit([](const auto&, int v) { EXPECT_EQ(v, 42); });

    EXPECT_FALSE(q.pop()); // queue now empty
}

namespace {
struct tiny_cfg {
    using RawMessage = pubsub::MessageContainer<int>;
    using Pool = typename RawMessage::Pool;
    using Message = pubsub::SharedMessage<RawMessage>;
    static constexpr size_t fast_queue_size = 4; // <= 4 → overflow easier
    static constexpr size_t max_subscribers = 8;
};
} // anonymous namespace

TEST(pubsub_queue_tests, push_pop_overflow) {
    tiny_cfg::Pool pool;
    pubsub::MessageQueue<tiny_cfg> q;

    constexpr int N = 20; // 16 will overflow the 4-slot ring
    for (int i = 0; i < N; ++i) {
        auto m = pool.get();
        m->set_data(i);
        q.push(m);
    }

    int cnt = 0;
    while (auto m = q.pop()) {
        (*m)->visit([&](const auto&, int v) {
            EXPECT_EQ(v, cnt++); // FIFO across fast+slow path
        });
    }
    EXPECT_EQ(cnt, N);
}

TEST(pubsub_topic_tests, fanout_two_subscribers) {
    using config = pubsub::MessageConfig<int>;
    config::Pool pool;

    // Two independent subscriber queues
    auto q1 = std::make_shared<pubsub::MessageQueue<config>>();
    auto q2 = std::make_shared<pubsub::MessageQueue<config>>();

    // Build topic tree: root → foo
    auto root = pubsub::get_top_level_topic<config>();
    auto* foo = root->get_or_create_sub_topic("foo");

    // Attach subscribers
    foo->add_subscriber(q1);
    foo->add_subscriber(q2);

    // Publish a message on /foo
    auto msg = pool.get();
    msg->set_data(123);
    foo->publish(msg);

    auto p1 = q1->pop();
    auto p2 = q2->pop();
    ASSERT_TRUE(p1 && p2);

    (*p1)->visit([](const auto& topic, int v) {
        EXPECT_EQ(topic.to_string(), "foo");
        EXPECT_EQ(v, 123);
    });
    (*p2)->visit([](const auto& topic, int v) {
        EXPECT_EQ(topic.to_string(), "foo");
        EXPECT_EQ(v, 123);
    });
}

TEST(pubsub_topic_tests, fanout_to_multiple_queues) {
    using cfg = pubsub::MessageConfig<int>;
    cfg::Pool pool;

    std::vector<std::shared_ptr<pubsub::MessageQueue<cfg>>> queues;
    for (int i = 0; i < 10; ++i) {
        queues.push_back(std::make_shared<pubsub::MessageQueue<cfg>>());
    }

    auto root = pubsub::get_top_level_topic<cfg>();
    auto* leaf = root->find({"foo", "bar"}); // creates /foo/bar

    for (auto& q : queues) {
        leaf->add_subscriber(q);
    }

    auto msg = pool.get();
    msg->set_data(99);
    leaf->publish(msg);

    for (auto& q : queues) {
        auto popped = q->pop();
        ASSERT_TRUE(popped);
        (*popped)->visit([](const auto& topic, int v) {
            EXPECT_EQ(topic.to_string(), "foo.bar");
            EXPECT_EQ(v, 99);
        });
    }
}

TEST(pubsub_topic_tests, duplicate_subscriber_means_duplicate_delivery) {
    using cfg = pubsub::MessageConfig<int>;
    cfg::Pool pool;
    auto q = std::make_shared<pubsub::MessageQueue<cfg>>();

    auto root = std::make_shared<pubsub::Topic<cfg>>("");
    auto* foo = root->get_or_create_sub_topic("dup");

    foo->add_subscriber(q);
    foo->add_subscriber(q); // add the same queue again

    auto msg = pool.get();
    msg->set_data(7);
    foo->publish(msg);

    int hits = 0;
    while (auto m = q->pop()) {
        ++hits;
    }
    EXPECT_EQ(hits, 2); // delivered twice
}

struct Collector
    : public pubsub::PublisherSubscriber<pubsub::MessageConfig<int>,
                                         Collector> {
    std::vector<int> seen;
    Collector() : PublisherSubscriber("collector") {
        subscribe({"ch"}); // listen on /ch
    }
    void on_message(const pubsub::TopicType&, int v) { seen.push_back(v); }
    template <typename T>
    void on_message(const pubsub::TopicType&, const T&) {} // ignore others
};

struct Pusher
    : public pubsub::PublisherSubscriber<pubsub::MessageConfig<int>, Pusher> {
    TopicKey key;
    Pusher() : PublisherSubscriber("pusher") { key = get_topic_key({"ch"}); }
    template <typename T> void on_message(const pubsub::TopicType&, const T&) {}
};

TEST(pubsub_ps_tests, manual_process_roundtrip) {
    Pusher pub;
    Collector sub;

    pub.publish(pub.key, 123); // enqueue into topic
    sub.process();             // manually drain sub’s queue

    ASSERT_EQ(sub.seen.size(), 1);
    EXPECT_EQ(sub.seen.front(), 123);
}