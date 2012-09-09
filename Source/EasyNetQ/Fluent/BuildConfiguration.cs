namespace EasyNetQ
{
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
}