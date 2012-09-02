namespace EasyNetQ
{
    public interface ISubscriberConfigurationBuilder
    {
        SubscriberConfiguration Build(BuildConfiguration buildConfiguration);

        ISubscriberConfigurationBuilder WithPrefetchCount(ushort prefetchCount);
    }
}