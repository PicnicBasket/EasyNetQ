using System.Text;
using Newtonsoft.Json;

namespace EasyNetQ
{
    public class JsonSerializer : ISerializer
    {
        readonly JsonSerializerSettings settings = new JsonSerializerSettings()
        {
            TypeNameHandling = TypeNameHandling.Auto,
            ReferenceLoopHandling = ReferenceLoopHandling.Ignore
        };

        public byte[] MessageToBytes<T>(T message)
        {
            return Encoding.UTF8.GetBytes(JsonConvert.SerializeObject(message, settings));
        }

        public T BytesToMessage<T>(byte[] bytes)
        {
            return JsonConvert.DeserializeObject<T>(Encoding.UTF8.GetString(bytes), settings);
        }
    }
}