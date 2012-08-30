using System.Collections.Specialized;

namespace EasyNetQ
{
    using System;
    using System.Collections.Generic;
    using System.Threading.Tasks;

    using EasyNetQ.Topology;

    public class SubscriberConfigurationBuilder
    {
        private readonly SerializeType serializeType;

        private readonly IEasyNetQLogger logger;

        private readonly ISerializer serializer;

        private readonly IConventions conventions;

        public SubscriberConfigurationBuilder(
            SerializeType serializeType,
            IEasyNetQLogger logger,
            ISerializer serializer,
            IConventions conventions)
        {
            this.serializeType = serializeType;
            this.logger = logger;
            this.serializer = serializer;
            this.conventions = conventions;
        }

        private Func<bool, IQueue> queueBuilder;

        private Func<Byte[], MessageProperties, MessageReceivedInfo, Task> onMessage;

        private readonly List<string> topics = new List<string>();

        private ushort? prefetchCount;

        private bool _isHa;

        public SubscriberConfigurationBuilder WithAsyncHandler<T>(Func<IMessage<T>, MessageReceivedInfo, Task> onMessage)
        {
            if (onMessage == null)
            {
                throw new ArgumentNullException("onMessage");
            }

            this.onMessage = (body, properties, messageRecievedInfo) =>
                {
                    this.CheckMessageType<T>(properties);

                    var messageBody = this.serializer.BytesToMessage<T>(body);
                    var message = new Message<T>(messageBody);
                    message.SetProperties(properties);
                    return onMessage(message, messageRecievedInfo);
                };

            return this;
        }

        private void CheckMessageType<TMessage>(MessageProperties properties)
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

        public SubscriberConfiguration Build()
        {
            var subscriberConfiguration = new SubscriberConfiguration
                {
                    Queue = this.queueBuilder(_isHa), 
                    OnMessage = onMessage,
                    IsHa = _isHa
                };

            if(this.prefetchCount != null)
            {
                subscriberConfiguration.PrefetchCount = this.prefetchCount.Value;
            }
            return subscriberConfiguration;
        }

        public SubscriberConfigurationBuilder WithSubscriptionId<T>(string subscriptionId)
        {
            this.queueBuilder = (isHa) =>
                {
                    var queueName = GetQueueName<T>(subscriptionId);
                    var exchangeName = GetExchangeName<T>();

                    IQueue queue;
                    if(_isHa)
                    {
                        queue = Queue.DeclareDurable(queueName, new Dictionary<string,object> { { "x-ha-policy", "all" } });
                    }
                    else
                    {
                        queue = Queue.DeclareDurable(queueName);
                    }

                    var exchange = Exchange.DeclareTopic(exchangeName);

                    var routingKeys = this.topics.Count > 0 ? this.topics.ToArray() : new[] { "#" };

                    queue.BindTo(exchange, routingKeys);

                    return queue;
                };
            return this;
        }

        private string GetExchangeName<T>()
        {
            return conventions.ExchangeNamingConvention(typeof(T));
        }

        private string GetQueueName<T>(string subscriptionId)
        {
            return conventions.QueueNamingConvention(typeof(T), subscriptionId);
        }

        public SubscriberConfigurationBuilder WithTopics(IEnumerable<string> topics)
        {
            this.topics.AddRange(topics);
            return this;
        }

        public SubscriberConfigurationBuilder WithTopic(string topic)
        {
            this.topics.Add(topic);
            return this;
        }

        public SubscriberConfigurationBuilder WithSyncHandler<T>(Action<T> onMessage)
        {
            this.WithAsyncHandler<T>((message, messageRecievedInfo) =>
            {
                var tcs = new TaskCompletionSource<object>();
                try
                {
                    onMessage(message.Body);
                    tcs.SetResult(null);
                }
                catch (Exception exception)
                {
                    tcs.SetException(exception);
                }
                return tcs.Task;
            });

            return this;
        }

        // todo: check that this isn't set simultaneously with SubscriberId
        public SubscriberConfigurationBuilder WithQueue(IQueue queue)
        {
            if (queue == null)
            {
                throw new ArgumentNullException("queue");
            }

            queueBuilder = (_) => queue;
            return this;
        }

        public SubscriberConfigurationBuilder WithPrefetchCount(ushort prefetchCount)
        {
           this.prefetchCount = prefetchCount;
            return this;
        }

        /// <summary>
        /// Configure this queue for High Availability
        /// </summary>
        /// <param name="isHa">true if the queue should be High Availability, false otherwise</param>
        public SubscriberConfigurationBuilder WithHa(bool isHa)
        {
            _isHa = isHa;
            return this;
        }
    }

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

        public bool IsHa { get; set; }
    }
}