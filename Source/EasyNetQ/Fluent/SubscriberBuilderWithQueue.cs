namespace EasyNetQ.Fluent
{
    using System;
    using System.Threading.Tasks;

    public class SubscriberBuilderWithQueue<T>
    {
        private readonly InternalSubscriberBuilder<T> internalSubscriberBuilder;

        internal SubscriberBuilderWithQueue(InternalSubscriberBuilder<T> internalSubscriberBuilder)
        {
            this.internalSubscriberBuilder = internalSubscriberBuilder;
        }

        public SubscriberBuilderComplete<T> Handler(Action<T> onMessage)
        {
            this.internalSubscriberBuilder.Handler(onMessage);
            return new SubscriberBuilderComplete<T>(this.internalSubscriberBuilder);
        }

        public SubscriberBuilderComplete<T> HandlerAsync(Func<T, Task> onMessage)
        {
            this.internalSubscriberBuilder.HandlerAsync(onMessage);
            return new SubscriberBuilderComplete<T>(this.internalSubscriberBuilder);
        }

        public SubscriberBuilderComplete<T> HandlerAsync(Func<byte[], MessageProperties, MessageReceivedInfo, Task> onMessage)
        {
            this.internalSubscriberBuilder.HandlerAsync(onMessage);
            return new SubscriberBuilderComplete<T>(this.internalSubscriberBuilder);
        }

        public SubscriberBuilderComplete<T> HandlerAsync(Func<IMessage<T>, MessageReceivedInfo, Task> onMessage)
        {
            this.internalSubscriberBuilder.HandlerAsync(onMessage);
            return new SubscriberBuilderComplete<T>(this.internalSubscriberBuilder);
        }

        public SubscriberBuilderWithQueue<T> WithPrefetchCount(ushort prefetchCount)
        {
            internalSubscriberBuilder.WithPrefetchCount(prefetchCount);
            return this;
        }
    }
}