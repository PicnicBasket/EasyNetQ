using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using EasyNetQ.Topology;

namespace EasyNetQ
{
    using EasyNetQ.Fluent;

    public class RabbitBus : IBus
    {
        private readonly SerializeType serializeType;
        private readonly IEasyNetQLogger logger;
		private readonly IConventions conventions;
        private readonly IAdvancedBus advancedBus;

        public const string RpcExchange = "easy_net_q_rpc";

        public SerializeType SerializeType
        {
            get { return serializeType; }
        }

        public IEasyNetQLogger Logger
        {
            get { return logger; }
        }

        public IConventions Conventions
        {
            get { return conventions; }
        }

        public RabbitBus(
            SerializeType serializeType, 
            IEasyNetQLogger logger,
			IConventions conventions, 
            IAdvancedBus advancedBus)
        {
            if(serializeType == null)
            {
                throw new ArgumentNullException("serializeType");
            }
            if(logger == null)
            {
                throw new ArgumentNullException("logger");
            }
            if(conventions == null)
            {
                throw new ArgumentNullException("conventions");
            }

            this.serializeType = serializeType;
            this.logger = logger;
			this.conventions = conventions;
            this.advancedBus = advancedBus;

            advancedBus.Connected += OnConnected;
            advancedBus.Disconnected += OnDisconnected;
        }

        public IPublishChannel OpenPublishChannel()
        {
            return new RabbitPublishChannel(this);
        }

        public void Subscribe<T>(string subscriptionId, Action<T> onMessage)
        {
            advancedBus.Subscribe<T>(b => b.Queue(subscriptionId).Handler(onMessage));
        }

        public void Subscribe<T>(string subscriptionId, string topic, Action<T> onMessage)
        {
            advancedBus.Subscribe<T>(b => b.Queue(subscriptionId, q => q.WithTopic(topic)).Handler(onMessage));
        }

        public void Subscribe<T>(string subscriptionId, IEnumerable<string> topics, Action<T> onMessage)
        {
            advancedBus.Subscribe<T>(b => b.Queue(subscriptionId, q => q.WithTopics(topics)).Handler(onMessage));
        }

        public void SubscribeAsync<T>(string subscriptionId, Func<T, Task> onMessage)
        {
            advancedBus.Subscribe<T>(b => b.Queue(subscriptionId).HandlerAsync(onMessage));
        }

        public void SubscribeAsync<T>(string subscriptionId, string topic, Func<T, Task> onMessage)
        {
            advancedBus.Subscribe<T>(b => b.Queue(subscriptionId, q => q.WithTopic(topic)).HandlerAsync(onMessage));
        }

        public void SubscribeAsync<T>(string subscriptionId, IEnumerable<string> topics, Func<T, Task> onMessage)
        {
            if (onMessage == null)
            {
                throw new ArgumentNullException("onMessage");
            }

            advancedBus.Subscribe<T>(b => b.Queue(subscriptionId, q => q.WithTopics(topics)).HandlerAsync(onMessage));
        }

        public void Subscribe<T>(Func<SubscriberBuilder<T>, SubscriberBuilderComplete<T>> configuration)
        {
            advancedBus.Subscribe(configuration);
        }

        public void Respond<TRequest, TResponse>(Func<TRequest, TResponse> responder)
        {
            if(responder == null)
            {
                throw new ArgumentNullException("responder");
            }

            Func<TRequest, Task<TResponse>> taskResponder = 
                request => Task<TResponse>.Factory.StartNew(_ => responder(request), null);

            RespondAsync(taskResponder);
        }

        public void RespondAsync<TRequest, TResponse>(Func<TRequest, Task<TResponse>> responder)
        {
            if (responder == null)
            {
                throw new ArgumentNullException("responder");
            }

            var requestTypeName = serializeType(typeof(TRequest));

            var exchange = Exchange.DeclareDirect(RpcExchange);
            var queue = Queue.DeclareDurable(requestTypeName);
            queue.BindTo(exchange, requestTypeName);

            advancedBus.Subscribe<TRequest>(queue, (requestMessage, messageRecievedInfo) =>
            {
                var tcs = new TaskCompletionSource<object>();

                responder(requestMessage.Body).ContinueWith(task =>
                {
                    if (task.IsFaulted)
                    {
                        Console.WriteLine("task faulted");
                        if (task.Exception != null)
                        {
                            tcs.SetException(task.Exception);
                        }
                    }
                    else
                    {
                        // check we're connected
                        while (!advancedBus.IsConnected)
                        {
                            Thread.Sleep(100);
                        }

                        var responseMessage = new Message<TResponse>(task.Result);
                        responseMessage.Properties.CorrelationId = requestMessage.Properties.CorrelationId;

                        using (var channel = advancedBus.OpenPublishChannel())
                        {
                            channel.Publish(Exchange.GetDefault(), requestMessage.Properties.ReplyTo, responseMessage);
                        }
                        tcs.SetResult(null);
                    }
                });

                return tcs.Task;
            });
        }

        public event Action Connected;

        protected void OnConnected()
        {
            if (Connected != null) Connected();
        }

        public event Action Disconnected;

        protected void OnDisconnected()
        {
            if (Disconnected != null) Disconnected();
        }

        public bool IsConnected
        {
            get { return advancedBus.IsConnected; }
        }

        public IAdvancedBus Advanced
        {
            get { return advancedBus; }
        }

        public void Dispose()
        {
            advancedBus.Dispose();
        }
    }
}