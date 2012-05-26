namespace EasyNetQ
{
    /// <summary>
    /// Used to specify how to dispatch messages to a consumer
    /// </summary>
    public enum DispatchType
    {
        // This is the default - each consumer prefetches 50 messages
        // prefetch messages is defined in RabbitAdvancedBus.prefetchCount
        Normal,
        // In fair mode consumers only get one message at a time.
        // Use this if processing a message takes a longer time compared to
        // the cost of asking the server for more messages. This mode 
        // will ensure that work is not queuing up on one worker node 
        // when other worker nodes are not busy.
        Fair
    }
}