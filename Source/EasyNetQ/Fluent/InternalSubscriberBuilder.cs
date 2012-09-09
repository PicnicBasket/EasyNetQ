namespace EasyNetQ.Fluent
{
    using System;
    using System.Threading.Tasks;

    using EasyNetQ.Topology;

    internal class InternalSubscriberBuilder<T>
    {
        private Func<BuildConfiguration, IQueue> buildQueue;

        private Func<BuildConfiguration, Func<byte[], MessageProperties, MessageReceivedInfo, Task>> buildMessageHandler;

        // prefetchCount determines how many messages will be allowed in the local in-memory queue
        // setting to zero makes this infinite, but risks an out-of-memory exception.
        // set the default to 50 based on this blog post:
        // http://www.rabbitmq.com/blog/2012/04/25/rabbitmq-performance-measurements-part-2/
        private ushort prefetchCount = 50;

        public InternalSubscriberBuilder<T> Queue(IQueue queue)
        {
            this.buildQueue = configuration => queue;
            return this;
        }

        public InternalSubscriberBuilder<T> Queue(string consumername, Action<QueueBuilder<T>> queueConfiguration = null)
        {
            var queueBuilder = new QueueBuilder<T>(consumername);

            if (queueConfiguration != null)
            {
                queueConfiguration(queueBuilder);
            }
            this.buildQueue = queueBuilder.Build;

            return this;
        }


        public InternalSubscriberBuilder<T> Handler(Action<T> onMessage)
        {
            return this.HandlerAsync(
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

        public InternalSubscriberBuilder<T> HandlerAsync(Func<T, Task> onMessage)
        {
            return this.HandlerAsync((message, messageInfo) => onMessage(message.Body));
        }

        public InternalSubscriberBuilder<T> HandlerAsync(Func<IMessage<T>, MessageReceivedInfo, Task> onMessage)
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

        public InternalSubscriberBuilder<T> HandlerAsync(Func<byte[], MessageProperties, MessageReceivedInfo, Task> onMessage)
        {
            this.buildMessageHandler = buildConfiguration => onMessage;
            return this;
        }

        public SubscriberConfiguration Build(BuildConfiguration buildConfiguration)
        {
            return new SubscriberConfiguration
                {
                    Queue = this.buildQueue(buildConfiguration),
                    OnMessage = this.buildMessageHandler(buildConfiguration),
                    PrefetchCount = this.prefetchCount
                };
        }

        public InternalSubscriberBuilder<T> WithPrefetchCount(ushort prefetchCount)
        {
            this.prefetchCount = prefetchCount;
            return this;
        }
    }
}