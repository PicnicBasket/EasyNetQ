using System;
using System.Threading.Tasks;

using EasyNetQ.Topology;

namespace EasyNetQ
{
    public class RawSubscriberBuilder : BaseSubscriberConfigurationBuilder, ISubscriberConfigurationBuilder
    {
        private readonly IQueue queue;

        private Func<byte[], MessageProperties, MessageReceivedInfo, Task> onMessage;

        public RawSubscriberBuilder(IQueue queue, Func<byte[], MessageProperties, MessageReceivedInfo, Task> onMessage)
        {
            this.queue = queue;
            this.onMessage = onMessage;
        }

        public SubscriberConfiguration Build(SerializeType serializeType, IEasyNetQLogger logger, ISerializer serializer, IConventions conventions)
        {
            var config = base.Build();
            config.Queue = queue;
            config.OnMessage = onMessage;
            return config;
        }
    }
}