using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace EasyNetQ
{
    public class SyncSubscriberBuilder<T> : BaseSubscriberConfigurationBuilder, ISubscriberConfigurationBuilder
    {
        private readonly string subscriptionId;

        private readonly Action<T> onMessage;

        public SyncSubscriberBuilder(string subscriptionId, Action<T> onMessage)
        {
            this.subscriptionId = subscriptionId;
            this.onMessage = onMessage;
        }

        public SyncSubscriberBuilder<T> WithTopics(IEnumerable<string> topics)
        {
            Topics.AddRange(topics);
            return this;
        }

        public SyncSubscriberBuilder<T> WithTopic(string topic)
        {
            Topics.Add(topic);
            return this;
        }

        public SyncSubscriberBuilder<T> WithPrefetchCount(ushort prefetchCount)
        {
            PrefetchCount = prefetchCount;
            return this;
        }

        /// <summary>
        /// Configure this queue for High Availability
        /// </summary>
        /// <param name="isHa">true if the queue should be High Availability, false otherwise</param>
        public SyncSubscriberBuilder<T> WithHa(bool isHa)
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
                    var task = CreateTask(message);
                    return task;
                };
            return config;
        }

        private Task CreateTask(Message<T> message)
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
        }
    }
}