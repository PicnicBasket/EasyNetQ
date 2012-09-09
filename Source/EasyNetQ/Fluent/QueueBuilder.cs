namespace EasyNetQ
{
    using System.Collections.Generic;

    using EasyNetQ.Topology;

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
            this.topics.Add(topic);
            return this;
        }

        public QueueBuilder<T> WithTopics(IEnumerable<string> topics)
        {
            this.topics.AddRange(topics);
            return this;
        }

        public IQueue Build(BuildConfiguration buildConfiguration)
        {
            var queueName = this.GetQueueName<T>(this.subscriptionId, buildConfiguration.Conventions);
            var exchangeName = this.GetExchangeName<T>(buildConfiguration.Conventions);

            IQueue queue;
            if (this.isHighlyAvailable)
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