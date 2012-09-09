namespace EasyNetQ.Fluent
{
    using System;

    using EasyNetQ.Topology;

    public class SubscriberBuilderWithHandler<T>
    {
        private readonly InternalSubscriberBuilder<T> internalSubscriberBuilder;

        internal SubscriberBuilderWithHandler(InternalSubscriberBuilder<T> internalSubscriberBuilder)
        {
            this.internalSubscriberBuilder = internalSubscriberBuilder;
        }

        public SubscriberBuilderComplete<T> Queue(IQueue queue)
        {
            this.internalSubscriberBuilder.Queue(queue);
            return new SubscriberBuilderComplete<T>(this.internalSubscriberBuilder);
        }

        public SubscriberBuilderComplete<T> Queue(string consumername, Action<QueueBuilder<T>> queueConfiguration = null)
        {
            this.internalSubscriberBuilder.Queue(consumername, queueConfiguration);
            return new SubscriberBuilderComplete<T>(this.internalSubscriberBuilder);
        }

        SubscriberBuilderWithHandler<T> WithPrefetchCount(ushort prefetchCount)
        {
            internalSubscriberBuilder.WithPrefetchCount(prefetchCount);
            return this;
        }
    }
}