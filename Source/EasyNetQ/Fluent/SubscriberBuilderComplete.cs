namespace EasyNetQ
{
    using EasyNetQ.Fluent;

    public class SubscriberBuilderComplete<T>
    {
        private readonly InternalSubscriberBuilder<T> internalSubscriberBuilder;

        internal SubscriberBuilderComplete(InternalSubscriberBuilder<T> internalSubscriberBuilder)
        {
            this.internalSubscriberBuilder = internalSubscriberBuilder;
        }

        internal SubscriberConfiguration Build(BuildConfiguration buildConfiguration)
        {
            return internalSubscriberBuilder.Build(buildConfiguration);
        }

        public SubscriberBuilderComplete<T> WithPrefetchCount(ushort prefetchCount)
        {
            internalSubscriberBuilder.WithPrefetchCount(prefetchCount);
            return this;
        }
    }
}