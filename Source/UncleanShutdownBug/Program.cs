using System;
using System.Threading;
using System.Threading.Tasks;
using EasyNetQ;
using EasyNetQ.Consumer;
using EasyNetQ.Tests;

namespace UncleanShutdownBug
{
    class Program
    {
        private static Random _random = new Random();
        static void Main(string[] args)
        {
            Action<IServiceRegister> registerServices = register =>
            {
                // see comments on the error strategy classes below
                // register.Register<IConsumerErrorStrategy, DoNothingErrorStrategy>();
                // register.Register<IConsumerErrorStrategy, AllowCleanShutdownErrorStrategy>();
            };
            var bus = RabbitHutch.CreateBus("host=localhost", registerServices);

            bus.SubscribeAsync<TestPerformanceMessage>("foo", MessageHandler);

            Thread.Sleep(TimeSpan.FromSeconds(10));

            bus.Dispose();
        }

        public static async Task MessageHandler(TestPerformanceMessage arg)
        {
            var delayUpToOneSecond = _random.Next(0, 1000);
            await Task.Delay(delayUpToOneSecond);
            throw new Exception("Handler failed");
        }
    }

    /// <summary>
    /// With this strategy the process always exits
    /// </summary>
    internal class DoNothingErrorStrategy : IConsumerErrorStrategy
    {
        public void Dispose()
        {
        }

        public PostExceptionAckStrategy HandleConsumerError(ConsumerExecutionContext context, Exception exception)
        {
            return PostExceptionAckStrategy.DoNothing;
        }
    }

    /// <summary>
    /// With this strategy the application seems MORE likely to exit but
    /// not always.
    /// </summary>
    internal class AllowCleanShutdownErrorStrategy : IConsumerErrorStrategy
    {
        private readonly DefaultConsumerErrorStrategy _defaultErrorStrategy;
        private bool _disposing;

        public AllowCleanShutdownErrorStrategy(IConnectionFactory connectionFactory,
            ISerializer serializer,
            IEasyNetQLogger logger,
            IConventions conventions,
            ITypeNameSerializer typeNameSerializer)
        {
            _defaultErrorStrategy = new DefaultConsumerErrorStrategy(connectionFactory, serializer, logger, conventions, typeNameSerializer);
        }

        public void Dispose()
        {
            _disposing = true;
            _defaultErrorStrategy.Dispose();
        }

        public PostExceptionAckStrategy HandleConsumerError(ConsumerExecutionContext context, Exception exception)
        {
            if (_disposing)
            {
                return PostExceptionAckStrategy.ShouldNackWithRequeue;
            }

            return _defaultErrorStrategy.HandleConsumerError(context, exception);
        }
    }
}
