using System.Collections.Generic;

using EasyNetQ.Topology;

namespace EasyNetQ
{
    public class BaseSubscriberConfigurationBuilder
    {
        private readonly List<string> topics = new List<string>();

        protected ushort? PrefetchCount { get; set; }

        protected List<string> Topics
        {
            get
            {
                return topics;
            }
        }

        protected bool IsHa
        {
            get
            {
                return _isHa;
            }
            set
            {
                _isHa = value;
            }
        }

        private bool _isHa;

        protected void CheckMessageType<TMessage>(MessageProperties properties, SerializeType serializeType, IEasyNetQLogger logger)
        {
            var typeName = serializeType(typeof(TMessage));
            if (properties.Type != typeName)
            {
                logger.ErrorWrite("Message type is incorrect. Expected '{0}', but was '{1}'",
                    typeName, properties.Type);

                throw new EasyNetQInvalidMessageTypeException("Message type is incorrect. Expected '{0}', but was '{1}'",
                    typeName, properties.Type);
            }
        }

        protected SubscriberConfiguration Build()
        {
            var subscriberConfiguration = new SubscriberConfiguration
                {
                    IsHa = _isHa
                };

            if (PrefetchCount != null)
            {
                subscriberConfiguration.PrefetchCount = PrefetchCount.Value;
            }
            return subscriberConfiguration;
        }

        protected IQueue BuildQueue<T>(string subscriptionId, IConventions conventions)
        {
            var queueName = GetQueueName<T>(subscriptionId, conventions);
            var exchangeName = GetExchangeName<T>(conventions);

            IQueue queue;
            if (_isHa)
            {
                queue = Queue.DeclareDurable(queueName, new Dictionary<string, object> { { "x-ha-policy", "all" } });
            }
            else
            {
                queue = Queue.DeclareDurable(queueName);
            }

            var exchange = Exchange.DeclareTopic(exchangeName);

            var routingKeys = this.topics.Count > 0 ? this.topics.ToArray() : new[] { "#" };

            queue.BindTo(exchange, routingKeys);

            return queue;
        }

        private string GetExchangeName<T>(IConventions conventions)
        {
            return conventions.ExchangeNamingConvention(typeof(T));
        }

        private string GetQueueName<T>(string subscriptionId, IConventions conventions)
        {
            return conventions.QueueNamingConvention(typeof(T), subscriptionId);
        }
    }
}