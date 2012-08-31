using System;
using System.Threading.Tasks;

using EasyNetQ.Topology;

namespace EasyNetQ
{
    public class RawQueueBuilder<T> : BaseSubscriberConfigurationBuilder, ISubscriberConfigurationBuilder
    {
        private readonly IQueue queue;

        private Func<IMessage<T>, MessageReceivedInfo, Task> onMessage;

        public RawQueueBuilder(IQueue queue, Func<IMessage<T>, MessageReceivedInfo, Task> onMessage)
        {
            this.queue = queue;
            this.onMessage = onMessage;
        }

        public SubscriberConfiguration Build(SerializeType serializeType, IEasyNetQLogger logger, ISerializer serializer, IConventions conventions)
        {
            var config = new SubscriberConfiguration
                {
                    Queue = queue,
                    OnMessage = (body, properties, messageRecievedInfo) =>
                        {
                            this.CheckMessageType<T>(properties, serializeType, logger);

                            var messageBody = serializer.BytesToMessage<T>(body);
                            var message = new Message<T>(messageBody);
                            message.SetProperties(properties);
                            return onMessage(message, messageRecievedInfo);
                        }
                };
            return config;
        }
    }
}