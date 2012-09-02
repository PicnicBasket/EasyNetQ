using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace EasyNetQ
{
    using EasyNetQ.Topology;

    /// <summary>
    /// Provides a simple Publish/Subscribe and Request/Response API for a message bus.
    /// </summary>
    public interface IBus : IDisposable
    {
        /// <summary>
        /// Opens and returns a new IPublishChannel. Note that IPublishChannel implements IDisposable
        /// and must be disposed.
        /// </summary>
        /// <returns>An IPublishChannel</returns>
        IPublishChannel OpenPublishChannel();

        /// <summary>
        /// Subscribes to a stream of messages that match a .NET type.
        /// </summary>
        /// <typeparam name="T">The type to subscribe to</typeparam>
        /// <param name="subscriptionId">
        /// A unique identifier for the subscription. Two subscriptions with the same subscriptionId
        /// and type will get messages delivered in turn. This is useful if you want multiple subscribers
        /// to load balance a subscription in a round-robin fashion.
        /// </param>
        /// <param name="onMessage">
        /// The action to run when a message arrives. When onMessage completes the message
        /// recipt is Ack'd. All onMessage delegates are processed on a single thread so you should
        /// avoid long running blocking IO operations. Consider using SubscribeAsync
        /// </param>
        void Subscribe<T>(string subscriptionId, Action<T> onMessage);

        /// <summary>
        /// Subscribes to a stream of messages that match a .NET type and the given topic
        /// </summary>
        /// <typeparam name="T">The type to subscribe to</typeparam>
        /// <param name="subscriptionId">
        /// A unique identifier for the subscription. Two subscriptions with the same subscriptionId
        /// and type will get messages delivered in turn. This is useful if you want multiple subscribers
        /// to load balance a subscription in a round-robin fashion.
        /// </param>
        /// <param name="topic">
        /// The topic to match on
        /// </param>
        /// <param name="onMessage">
        /// The action to run when a message arrives. When onMessage completes the message
        /// recipt is Ack'd. All onMessage delegates are processed on a single thread so you should
        /// avoid long running blocking IO operations. Consider using SubscribeAsync
        /// </param>
        void Subscribe<T>(string subscriptionId, string topic, Action<T> onMessage);

        /// <summary>
        /// Subscribes to a stream of messages that match a .NET type and the given topic
        /// </summary>
        /// <typeparam name="T">The type to subscribe to</typeparam>
        /// <param name="subscriptionId">
        /// A unique identifier for the subscription. Two subscriptions with the same subscriptionId
        /// and type will get messages delivered in turn. This is useful if you want multiple subscribers
        /// to load balance a subscription in a round-robin fashion.
        /// </param>
        /// <param name="topics">
        /// The topics to match on. Each topic string creates a new binding between the exchange and 
        /// subscription queue.
        /// </param>
        /// <param name="onMessage">
        /// The action to run when a message arrives. When onMessage completes the message
        /// recipt is Ack'd. All onMessage delegates are processed on a single thread so you should
        /// avoid long running blocking IO operations. Consider using SubscribeAsync
        /// </param>
        void Subscribe<T>(string subscriptionId, IEnumerable<string> topics, Action<T> onMessage);

        /// <summary>
        /// Subscribes to a stream of messages that match a .NET type.
        /// Allows the subscriber to complete asynchronously.
        /// </summary>
        /// <typeparam name="T">The type to subscribe to</typeparam>
        /// <param name="subscriptionId">
        /// A unique identifier for the subscription. Two subscriptions with the same subscriptionId
        /// and type will get messages delivered in turn. This is useful if you want multiple subscribers
        /// to load balance a subscription in a round-robin fashion.
        /// </param>
        /// <param name="onMessage">
        /// The action to run when a message arrives. onMessage can immediately return a Task and
        /// then continue processing asynchronously. When the Task completes the message will be
        /// Ack'd.
        /// </param>
        void SubscribeAsync<T>(string subscriptionId, Func<T, Task> onMessage);

        /// <summary>
        /// Subscribes to a stream of messages that match a .NET type.
        /// Allows the subscriber to complete asynchronously.
        /// </summary>
        /// <typeparam name="T">The type to subscribe to</typeparam>
        /// <param name="subscriptionId">
        /// A unique identifier for the subscription. Two subscriptions with the same subscriptionId
        /// and type will get messages delivered in turn. This is useful if you want multiple subscribers
        /// to load balance a subscription in a round-robin fashion.
        /// </param>
        /// <param name="topic">
        /// The topic to match on
        /// </param>
        /// <param name="onMessage">
        /// The action to run when a message arrives. onMessage can immediately return a Task and
        /// then continue processing asynchronously. When the Task completes the message will be
        /// Ack'd.
        /// </param>
        void SubscribeAsync<T>(string subscriptionId, string topic, Func<T, Task> onMessage);

        /// <summary>
        /// Subscribes to a stream of messages that match a .NET type.
        /// Allows the subscriber to complete asynchronously.
        /// </summary>
        /// <typeparam name="T">The type to subscribe to</typeparam>
        /// <param name="subscriptionId">
        /// A unique identifier for the subscription. Two subscriptions with the same subscriptionId
        /// and type will get messages delivered in turn. This is useful if you want multiple subscribers
        /// to load balance a subscription in a round-robin fashion.
        /// </param>
        /// <param name="topics">
        /// The topics to match on. Each topic string creates a new binding between the queue and the
        /// exchange.
        /// </param>
        /// <param name="onMessage">
        /// The action to run when a message arrives. onMessage can immediately return a Task and
        /// then continue processing asynchronously. When the Task completes the message will be
        /// Ack'd.
        /// </param>
        void SubscribeAsync<T>(string subscriptionId, IEnumerable<string> topics, Func<T, Task> onMessage);

        /// <summary>
        /// Responds to an RPC request.
        /// </summary>
        /// <typeparam name="TRequest">The request type.</typeparam>
        /// <typeparam name="TResponse">The response type.</typeparam>
        /// <param name="responder">
        /// A function to run when the request is received. It should return the response.
        /// </param>
        void Respond<TRequest, TResponse>(Func<TRequest, TResponse> responder);

        /// <summary>
        /// Responds to an RPC request asynchronously.
        /// </summary>
        /// <typeparam name="TRequest">The request type.</typeparam>
        /// <typeparam name="TResponse">The response type</typeparam>
        /// <param name="responder">
        /// A function to run when the request is received.
        /// </param>
        void RespondAsync<TRequest, TResponse>(Func<TRequest, Task<TResponse>> responder);

        /// <summary>
        /// Fires once the bus has connected to a RabbitMQ server.
        /// </summary>
        event Action Connected;

        /// <summary>
        /// Fires when the bus disconnects from a RabbitMQ server.
        /// </summary>
        event Action Disconnected;

        /// <summary>
        /// True if the bus is connected, False if it is not.
        /// </summary>
        bool IsConnected { get; }

        /// <summary>
        /// Return the advanced EasyNetQ advanced API.
        /// </summary>
        IAdvancedBus Advanced { get; }

        /// <summary>
        /// Subscribe to a a queue on the bus
        /// </summary>
        /// <param name="configuration">Subscriber configuration</param>
        void Subscribe<T>(Func<ISubscriberConfigurer<T>, ISubscriberConfigurationBuilder> configuration);
    }

    public interface ISubscriberConfigurer<T>
    {
        IConfigurationWithQueue<T> Queue(IQueue queue);

        IConfigurationWithQueue<T> Queue(string consumername, Action<QueueBuilder<T>> queueConfiguration = null);

        ISubscriberConfigurer<T> WithPrefetchCount(ushort prefetchCount);
    }

    public class QueueBuilder<T>
    {
        private readonly string subscriptionId;

        public QueueBuilder(string subscriptionId)
        {
            this.subscriptionId = subscriptionId;
        }

        private bool isHighlyAvailable;

        private readonly List<string> topics = new List<string>();

        public QueueBuilder<T> HighAvailability(bool isHighlyAvailable)
        {
            this.isHighlyAvailable = isHighlyAvailable;
            return this;
        }

        public QueueBuilder<T> WithTopic(string topic)
        {
            topics.Add(topic);
            return this;
        }

        public QueueBuilder<T> WithTopics(IEnumerable<string> topics)
        {
            this.topics.AddRange(topics);
            return this;
        }

        public IQueue Build(BuildConfiguration buildConfiguration)
        {
            var queueName = GetQueueName<T>(subscriptionId, buildConfiguration.Conventions);
            var exchangeName = GetExchangeName<T>(buildConfiguration.Conventions);

            IQueue queue;
            if (isHighlyAvailable)
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

    public class SubscriberConfigurer<T> : ISubscriberConfigurer<T>, ISubscriberConfigurationBuilder, IConfigurationWithQueue<T>
    {
        private Func<BuildConfiguration, IQueue> buildQueue;

        private Func<BuildConfiguration, Func<byte[], MessageProperties, MessageReceivedInfo, Task>> buildMessageHandler;

        // prefetchCount determines how many messages will be allowed in the local in-memory queue
        // setting to zero makes this infinite, but risks an out-of-memory exception.
        // set the default to 50 based on this blog post:
        // http://www.rabbitmq.com/blog/2012/04/25/rabbitmq-performance-measurements-part-2/
        private ushort prefetchCount = 50; 

        public IConfigurationWithQueue<T> Queue(IQueue queue)
        {
            buildQueue = configuration => queue;
            return this;
        }

        public IConfigurationWithQueue<T> Queue(string consumername, Action<QueueBuilder<T>> queueConfiguration = null)
        {
            var queueBuilder = new QueueBuilder<T>(consumername);

            if (queueConfiguration != null)
            {
                queueConfiguration(queueBuilder);
            }
            buildQueue = queueBuilder.Build;

            return this;
        }


        public ISubscriberConfigurationBuilder Handler(Action<T> onMessage)
        {
            return HandlerAsync(
                t =>
                    {
                        var tcs = new TaskCompletionSource<object>();
                        try
                        {
                            onMessage(t);
                            tcs.SetResult(null);
                        }
                        catch (Exception exception)
                        {
                            tcs.SetException(exception);
                        }
                        return tcs.Task;

                    });
        }

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

        public ISubscriberConfigurationBuilder HandlerAsync(Func<T, Task> onMessage)
        {
            return HandlerAsync((message, messageInfo) => onMessage(message.Body));
        }

        public ISubscriberConfigurationBuilder HandlerAsync(Func<IMessage<T>, MessageReceivedInfo, Task> onMessage)
        {
            this.buildMessageHandler = buildConfiguration => (body, properties, messageRecievedInfo) =>
            {
                this.CheckMessageType<T>(properties, buildConfiguration.SerializeType, buildConfiguration.Logger);

                var messageBody = buildConfiguration.Serializer.BytesToMessage<T>(body);
                var message = new Message<T>(messageBody);
                message.SetProperties(properties);
                return onMessage(message, messageRecievedInfo);
            };
            return this;
        }

        public ISubscriberConfigurationBuilder HandlerAsync(Func<byte[], MessageProperties, MessageReceivedInfo, Task> onMessage)
        {
            this.buildMessageHandler = buildConfiguration => onMessage;
            return this;
        }

        SubscriberConfiguration ISubscriberConfigurationBuilder.Build(BuildConfiguration buildConfiguration)
        {
            return new SubscriberConfiguration
            {
                Queue = buildQueue(buildConfiguration),
                OnMessage = buildMessageHandler(buildConfiguration),
                PrefetchCount = this.prefetchCount
            };
        }

        ISubscriberConfigurer<T> ISubscriberConfigurer<T>.WithPrefetchCount(ushort prefetchCount)
        {
            this.prefetchCount = prefetchCount;
            return this;
        }

        ISubscriberConfigurationBuilder ISubscriberConfigurationBuilder.WithPrefetchCount(ushort prefetchCount)
        {
            this.prefetchCount = prefetchCount;
            return this;
        }
    }

    public class BuildConfiguration
    {
        private SerializeType serializeType;

        private IEasyNetQLogger logger;

        private ISerializer serializer;

        private IConventions conventions;

        public BuildConfiguration(SerializeType serializeType, IEasyNetQLogger logger, ISerializer serializer, IConventions conventions)
        {
            this.serializeType = serializeType;
            this.logger = logger;
            this.serializer = serializer;
            this.conventions = conventions;
        }

        public SerializeType SerializeType
        {
            get
            {
                return this.serializeType;
            }
        }

        public IEasyNetQLogger Logger
        {
            get
            {
                return this.logger;
            }
        }

        public ISerializer Serializer
        {
            get
            {
                return this.serializer;
            }
        }

        public IConventions Conventions
        {
            get
            {
                return this.conventions;
            }
        }
    }


    public interface IConfigurationWithQueue<T>
    {
        ISubscriberConfigurationBuilder Handler(Action<T> onMessage);

        ISubscriberConfigurationBuilder HandlerAsync(Func<T, Task> onMessage);

        ISubscriberConfigurationBuilder HandlerAsync(Func<byte[], MessageProperties, MessageReceivedInfo, Task> onMessage);

        ISubscriberConfigurationBuilder HandlerAsync(Func<IMessage<T>, MessageReceivedInfo, Task> onMessage);
    }
}