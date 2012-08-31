using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace EasyNetQ
{
    public class AsyncSubscriberBuilder<T> : BaseSubscriberConfigurationBuilder, ISubscriberConfigurationBuilder
    {
        private readonly string subscriptionId;

        private Func<IMessage<T>, MessageReceivedInfo, Task> onMessage;

        private AsyncSubscriberBuilder(string subscriptionId)
        {
            if (subscriptionId == null)
            {
                throw new ArgumentNullException("subscriptionId");
            }

            this.subscriptionId = subscriptionId; ;
        } 

        public AsyncSubscriberBuilder(string subscriptionId, Func<IMessage<T>, MessageReceivedInfo, Task> onMessage) : this(subscriptionId)
        {
            if (onMessage == null)
            {
                throw new ArgumentNullException("onMessage");
            }

            this.onMessage = onMessage;
            this.subscriptionId = subscriptionId;
        }

        public AsyncSubscriberBuilder(string subscriptionId, Func<T, Task> onMessage) : this(subscriptionId)
        {
            if (onMessage == null)
            {
                throw new ArgumentNullException("onMessage");
            }

            this.onMessage = (message, messageInfo) => { return onMessage(message.Body); };
            this.subscriptionId = subscriptionId;
        }

        public AsyncSubscriberBuilder<T> WithTopics(IEnumerable<string> topics)
        {
            Topics.AddRange(topics);
            return this;
        }

        public AsyncSubscriberBuilder<T> WithTopic(string topic)
        {
            Topics.Add(topic);
            return this;
        }

        public AsyncSubscriberBuilder<T> WithPrefetchCount(ushort prefetchCount)
        {
            PrefetchCount = prefetchCount;
            return this;
        }

        /// <summary>
        /// Configure this queue for High Availability
        /// </summary>
        /// <param name="isHa">true if the queue should be High Availability, false otherwise</param>
        public AsyncSubscriberBuilder<T> WithHa(bool isHa)
        {
            IsHa = isHa;
            return this;
        }

        public SubscriberConfiguration Build(SerializeType serializeType, IEasyNetQLogger logger, ISerializer serializer, IConventions conventions)
        {
            var config = base.Build();
            config.Queue = base.BuildQueue<T>(this.subscriptionId, conventions);
            config.OnMessage = (body, properties, messageRecievedInfo) =>
                {
                    this.CheckMessageType<T>(properties, serializeType, logger);

                    var messageBody = serializer.BytesToMessage<T>(body);
                    var message = new Message<T>(messageBody);
                    message.SetProperties(properties);
                    return onMessage(message, messageRecievedInfo);
                };
            return config;
        }
    }
}