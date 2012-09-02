namespace EasyNetQ.Tests
{
    using System.Threading.Tasks;

    using EasyNetQ.Topology;

    using NUnit.Framework;

    [TestFixture]
    public class FluentApiTests
    {
        [Test]
        public void StandardSynchronousSubscriber()
        {
            var bus = Rhino.Mocks.MockRepository.GenerateStub<IBus>();

            bus.Subscribe<TestMessage>(b => b.Queue("consumerName").Handler(x => { }));
        }

        [Test]
        public void StandardSynchronousSubscriberWithQueueTopic()
        {
            var bus = Rhino.Mocks.MockRepository.GenerateStub<IBus>();

            bus.Subscribe<TestMessage>(b =>
                b.Queue("consumerName", x => x.WithTopic("topicName")
                ).Handler(x => { }));
        }

        [Test]
        public void StandardSynchronousSubscriberWithQueueTopics()
        {
            var bus = Rhino.Mocks.MockRepository.GenerateStub<IBus>();

            bus.Subscribe<TestMessage>(b =>
                b.Queue("consumerName", x => x.WithTopics(new[] { "topicName" }))
                 .Handler(x => { }));
        }

        [Test]
        public void StandardSynchronousSubscriberWithPrefetchCount()
        {
            var bus = Rhino.Mocks.MockRepository.GenerateStub<IBus>();

            bus.Subscribe<TestMessage>(b =>
                b.Queue("consumerName")
                 .Handler(x => { })
                 .WithPrefetchCount(1));
        }


        [Test]
        public void StandardSynchronousSubscriberWithMultipleQueueOptions()
        {
            var bus = Rhino.Mocks.MockRepository.GenerateStub<IBus>();

            bus.Subscribe<TestMessage>(b =>
                b.Queue("consumerName",
                  queue => queue.HighAvailability(true).WithTopic("topicName")
                ).Handler(x => { }));
        }

        [Test]
        public void StandardAsyncSubscriber()
        {
            var bus = Rhino.Mocks.MockRepository.GenerateStub<IBus>();

            bus.Subscribe<TestMessage>(b => b.Queue("consumerName").HandlerAsync(x => new Task(() => { })));
        }

        [Test]
        public void WithExplicitQueue()
        {
            var bus = Rhino.Mocks.MockRepository.GenerateStub<IBus>();

            var queue = Rhino.Mocks.MockRepository.GenerateStub<IQueue>();
            bus.Subscribe<TestMessage>(b => b.Queue(queue).Handler(x => { }));
        }

        [Test]
        public void WithRawMessageAsyncHandler()
        {
            var bus = Rhino.Mocks.MockRepository.GenerateStub<IBus>();

            bus.Subscribe<TestMessage>(
                b => b
                    .Queue("consumerName")
                    .HandlerAsync(
                      (rawBytes, messageProperties, messageReceivedInfo) =>
                          new Task(() =>
                              {

                              })));
        }

        [Test]
        public void WithRawMessageHandler()
        {
            var bus = Rhino.Mocks.MockRepository.GenerateStub<IBus>();

            bus.Subscribe<TestMessage>(
                b => b
                    .Queue("consumerName")
                    .HandlerAsync(
                      (rawBytes, messageProperties, messageReceivedInfo) => new Task(() => { })));
        }

        [Test]
        public void WithMessageProperties()
        {
            var bus = Rhino.Mocks.MockRepository.GenerateStub<IBus>();

            TestMessage received = null;
            bus.Subscribe<TestMessage>(
                b => b
                    .Queue("consumerName")
                    .HandlerAsync(
                      (message, messageReceivedInfo) => new Task(() =>
                          {
                              received = message.Body;
                          })));
        }
    }
}