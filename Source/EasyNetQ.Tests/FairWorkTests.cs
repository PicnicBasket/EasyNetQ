// ReSharper disable InconsistentNaming
namespace EasyNetQ.Tests
{
    using System;
    using System.Collections.Generic;
    using System.Threading;
    using System.Threading.Tasks;

    using NUnit.Framework;

    [TestFixture]
    public class FairWorkTests
    {
        private IBus bus;
        private List<IBus> _busList = new List<IBus>();

        [SetUp]
        public void SetUp()
        {
            this.bus = RabbitHutch.CreateBus("host=localhost");
            while (!this.bus.IsConnected) Thread.Sleep(10);
        }

        [TearDown]
        public void TearDown()
        {
            if (this.bus != null) this.bus.Dispose();
            foreach (var childBus in this._busList)
            {
                childBus.Dispose();
            }
        }

        [Test, Explicit("Needs a Rabbit instance on localhost to work")]
        public void SettingPrefetchTo1_DistributesWorkFairly()
        {
            // NOTE: This test will fail when the prefetchCount = 50
            // change it to 1 in RabbitAdvancedBus to see this test pass

            var workItemsProcessed = 0;

            // create two consumers that will sleep for the worktime specified
            for (var consumerIndex = 0; consumerIndex < 2; consumerIndex++)
            {
                var consumerBus = RabbitHutch.CreateBus("host=localhost");
                this._busList.Add(consumerBus);
                consumerBus.Subscribe<WorkItem>(b => b.
                    Queue("consumer").
                    Handler(
                        workItem =>
                        {
                            Thread.Sleep(TimeSpan.FromSeconds(workItem.WorkTime));
                            // ReSharper disable AccessToModifiedClosure - in this case it's what we want
                            Interlocked.Increment(ref workItemsProcessed);
                            // ReSharper restore AccessToModifiedClosure
                        }).
                    WithPrefetchCount(1));
            }

            // create six uneven work items
            const int WorkItemCount = 6;
            using (var publishChannel = this.bus.OpenPublishChannel())
            {
                publishChannel.Publish(new WorkItem { WorkTime = 0 });
                publishChannel.Publish(new WorkItem { WorkTime = 5 });
                publishChannel.Publish(new WorkItem { WorkTime = 0 });
                publishChannel.Publish(new WorkItem { WorkTime = 5 });
                publishChannel.Publish(new WorkItem { WorkTime = 0 });
                publishChannel.Publish(new WorkItem { WorkTime = 5 });
            }

            var startTime = DateTime.Now;
            // wait for all items to be processed
            while (workItemsProcessed < WorkItemCount)
            {
                Thread.Sleep(100);
            }
            var endTime = DateTime.Now;
            var totalTimeInSeconds = (endTime - startTime).Seconds;

            // if the items were fairly distributed then a single consumer would take at most 
            // 10 seconds. Check for 11 to be safe. If they queues are unfair someone might get all three.
            Assert.That(totalTimeInSeconds, Is.LessThan(11));
        }

        [Test, Explicit("Needs a Rabbit instance on localhost to work")]
        public void SubscribersWithMixedDistributionTypes()
        {
            using (new TestSubscriber(DispatchType.Normal))
            {
                // this is so the subscriber queue is created
                // the real subscribers are created below
                // after all the work has been published
            }

            const int WorkItemCount = 55;
            using (var publishChannel = this.bus.OpenPublishChannel())
            {
                for (int count = 0; count < WorkItemCount; count++)
                {
                    publishChannel.Publish(new WorkItem { WorkTime = 0 });
                }
            }

            var subsciber2 = new TestSubscriber(DispatchType.Fair);
            var subsciber1 = new TestSubscriber(DispatchType.Normal);

            int totalProcessed;

            // Wait for messages to be consumed
            TimeSpan processingTime = TimeSpan.FromSeconds(0);
            var startTime = DateTime.Now;
            var totalWaitTime = TimeSpan.FromSeconds(10);
            do
            {
                totalProcessed = subsciber1.WorkItemsProcessed + subsciber2.WorkItemsProcessed;
                processingTime = DateTime.Now.Subtract(startTime);
                Thread.Sleep(TimeSpan.FromMilliseconds(100));
            }
            while (totalProcessed < WorkItemCount && processingTime < totalWaitTime);

            Console.WriteLine("Normal: " + subsciber1.WorkItemsProcessed);
            Console.WriteLine("Fair: " + subsciber2.WorkItemsProcessed);
            Assert.That(totalProcessed, Is.EqualTo(WorkItemCount));

            // Normal will take at least the first 50, sometime more depending on time take to retrieve messages
            Assert.That(subsciber1.WorkItemsProcessed, Is.GreaterThanOrEqualTo(50));
            // Fair subscriber will get the leftovers
            Assert.That(subsciber2.WorkItemsProcessed, Is.GreaterThan(0));
        }

        public class WorkItem
        {
            public int WorkTime { get; set; }
        }
    }

    public enum DispatchType
    {
        Normal,
        Fair
    }

    public class TestSubscriber : IDisposable
    {
        private int workItemsProcessed;

        public int WorkItemsProcessed { get { return workItemsProcessed; } }
        private readonly IBus bus;

        public TestSubscriber(DispatchType dispatchType)
        {
            this.bus = RabbitHutch.CreateBus("host=localhost");

            this.bus.Subscribe<FairWorkTests.WorkItem>(b =>
                {
                    var config = b.Queue("consumer").Handler(
                        workItem =>
                            {
                                Thread.Sleep(TimeSpan.FromSeconds(workItem.WorkTime));
                                Interlocked.Increment(ref this.workItemsProcessed);
                            });
                    if (dispatchType == DispatchType.Fair)
                    {
                        config.WithPrefetchCount(1);
                    }

                    return config;
                });
        }

        public void Dispose()
        {
            this.bus.Dispose();
        }
    }
}

// ReSharper restore InconsistentNaming