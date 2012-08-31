namespace EasyNetQ
{
    public interface ISubscriberConfigurationBuilder
    {
        SubscriberConfiguration Build(SerializeType serializeType, IEasyNetQLogger logger, ISerializer serializer, IConventions conventions);
    }
}