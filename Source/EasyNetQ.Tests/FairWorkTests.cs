// ReSharper disable InconsistentNaming
namespace EasyNetQ.Tests
{
    using System;
    using System.Collections.Generic;
    using System.Threading;

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
        public void ProofOfConcept_SettingPrefetchTo1_DistributesWorkFairly()
        {
            // NOTE: This test will fail when the prefetchCount = 50
            // change it to 1 in RabbitAdvancedBus to see this test pass

            var workItemsProcessed = 0;
           
            // create two consumers that will sleep for the worktime specified
            for (var consumerIndex = 0; consumerIndex < 2; consumerIndex++)
            {
                var consumerBus = RabbitHutch.CreateBus("host=localhost");
                this._busList.Add(consumerBus);
                consumerBus.Subscribe<WorkItem>("consumer", workItem =>
                    {
                        Thread.Sleep(TimeSpan.FromSeconds(workItem.WorkTime));
// ReSharper disable AccessToModifiedClosure - in this case it's what we want
                        Interlocked.Increment(ref workItemsProcessed);
// ReSharper restore AccessToModifiedClosure
                    });
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

        public class WorkItem
        {
            public int WorkTime { get; set; }
        }
    }
}

// ReSharper restore InconsistentNaming