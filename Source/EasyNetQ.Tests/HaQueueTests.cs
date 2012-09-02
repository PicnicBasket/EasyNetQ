using System;
using System.Threading;

using NUnit.Framework;

namespace EasyNetQ.Tests
{
    [TestFixture]
    public class HaQueueTests
    {
        [Test, Explicit("Needs a Rabbit instance on localhost to work")]
        public void CanCreateHaQueue()
        {
            var called = false;
            var bus = RabbitHutch.CreateBus("host=localhost");
            
            bus.Subscribe<HaMessage>(b =>
                    b.Queue("Test", q => q.HighAvailability(true))
                     .Handler( _ => { called = true; })
                );

            using(var publishChannel = bus.OpenPublishChannel())
            {
                publishChannel.Publish(new HaMessage());
            }

            Thread.Sleep(TimeSpan.FromMilliseconds(500));

            // todo andrew browne: this assert doesn't actualy check that the queue is HA just that its creation worked.
            Assert.True(called);
        }

        public class HaMessage { }
    }
}