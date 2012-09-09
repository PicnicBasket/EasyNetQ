namespace EasyNetQ.Fluent
{
    using System;
    using System.Threading.Tasks;

    using EasyNetQ.Topology;

    public class SubscriberBuilder<T>
    {
        private readonly InternalSubscriberBuilder<T> internalSubscriberBuilder;

        internal SubscriberBuilder(InternalSubscriberBuilder<T> internalSubscriberBuilder)
        {
            this.internalSubscriberBuilder = internalSubscriberBuilder;
        }

        public SubscriberBuilderWithQueue<T> Queue(IQueue queue)
        {
            this.internalSubscriberBuilder.Queue(queue);
            return new SubscriberBuilderWithQueue<T>(this.internalSubscriberBuilder);
        }

        public SubscriberBuilderWithQueue<T> Queue(string consumername, Action<QueueBuilder<T>> queueConfiguration = null)
        {
            this.internalSubscriberBuilder.Queue(consumername, queueConfiguration);
            return new SubscriberBuilderWithQueue<T>(this.internalSubscriberBuilder);
        }

        public SubscriberBuilderWithHandler<T> Handler(Action<T> onMessage)
        {
            this.internalSubscriberBuilder.Handler(onMessage);
            return new SubscriberBuilderWithHandler<T>(this.internalSubscriberBuilder);
        }

        public SubscriberBuilderWithHandler<T> HandlerAsync(Func<T, Task> onMessage)
        {
            this.internalSubscriberBuilder.HandlerAsync(onMessage);
            return new SubscriberBuilderWithHandler<T>(this.internalSubscriberBuilder);
        }

        public SubscriberBuilderWithHandler<T> HandlerAsync(Func<byte[], MessageProperties, MessageReceivedInfo, Task> onMessage)
        {
            this.internalSubscriberBuilder.HandlerAsync(onMessage);
            return new SubscriberBuilderWithHandler<T>(this.internalSubscriberBuilder);
        }

        public SubscriberBuilderWithHandler<T> HandlerAsync(Func<IMessage<T>, MessageReceivedInfo, Task> onMessage)
        {
            this.internalSubscriberBuilder.HandlerAsync(onMessage);
            return new SubscriberBuilderWithHandler<T>(this.internalSubscriberBuilder);
        }

        public SubscriberBuilder<T> WithPrefetchCount(ushort prefetchCount)
        {
            internalSubscriberBuilder.WithPrefetchCount(prefetchCount);
            return this;
        }
    }
}