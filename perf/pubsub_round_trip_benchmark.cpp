#include <benchmark/benchmark.h>

#include <pubsub/pubsub.hpp>

using config = pubsub::MessageConfig<size_t, std::string>;

class EchoClient : public pubsub::PublisherSubscriber<config, EchoClient> {
  private:
    TopicKey m_send_topic;

  public:
    EchoClient(pubsub::TopicType send_topic, pubsub::TopicType recv_topic)
        : pubsub::PublisherSubscriber<config, EchoClient>("EchoClient"),
          m_send_topic(get_topic_key(send_topic)) {
        subscribe(recv_topic);
    }

    template <typename T>
    void on_message(const pubsub::TopicType& topic, const T& data) {
        publish(m_send_topic, data);
    }
};

class RoundTripClient
    : public pubsub::PublisherSubscriber<config, RoundTripClient> {
  private:
    TopicKey m_send_topic;
    bool m_recvd_message = false;
    size_t m_sent_message = 0;

  public:
    RoundTripClient(pubsub::TopicType send_topic, pubsub::TopicType recv_topic)
        : pubsub::PublisherSubscriber<config, RoundTripClient>(
              "RoundTripClient"),
          m_send_topic(get_topic_key(send_topic)) {
        subscribe(recv_topic);
    }

    void send_message(size_t message) {
        m_sent_message = message;
        m_recvd_message = false;
        publish(m_send_topic, message);
    }

    void on_message(const pubsub::TopicType& topic, const size_t& data) {
        if (m_recvd_message)
            return;

        if (m_sent_message == data) {
            m_recvd_message = true;
        }
    }

    template <typename T>
    void on_message(const pubsub::TopicType& topic, const T& data) {}

    void run_round_trip(size_t message) {
        send_message(message);
        while (!m_recvd_message)
            this->process();
    }
};

static void roundtrip(benchmark::State& st) {
    pubsub::TopicType out_topic({"out"});
    pubsub::TopicType in_topic({"in"});

    EchoClient echo(in_topic, out_topic);
    echo.launch();

    RoundTripClient client(out_topic, in_topic);

    client.on_launch();

    size_t iteration = 0;
    for (auto _ : st) {
        client.run_round_trip(iteration);
        iteration++;
    }

    client.on_close();

    st.SetItemsProcessed(st.iterations());
}

BENCHMARK(roundtrip)->Args({});

BENCHMARK_MAIN();