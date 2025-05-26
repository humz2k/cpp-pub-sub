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