namespace EasyNetQ
{
    using System;
    using System.Threading.Tasks;

    using EasyNetQ.Topology;

    public class SubscriberConfiguration
    {
        // prefetchCount determines how many messages will be allowed in the local in-memory queue
        // setting to zero makes this infinite, but risks an out-of-memory exception.
        // set to 50 based on this blog post:
        // http://www.rabbitmq.com/blog/2012/04/25/rabbitmq-performance-measurements-part-2/
        private const int defaultPrefetchCount = 50;

        public SubscriberConfiguration()
        {
            this.PrefetchCount = defaultPrefetchCount;
        }

        public IQueue Queue { get; set; }

        public Func<byte[], MessageProperties, MessageReceivedInfo, Task> OnMessage { get; set; }

        public ushort PrefetchCount { get; set; }
    }
}