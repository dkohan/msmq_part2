using Microsoft.Extensions.Logging;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System;
using System.IO;
using System.Text;

namespace Server
{
    public class Program
    {
        private static ILogger _logger;
        private static string _path= "C:\\TestMessagingFolder";
        private static string _queueName = ".\\Private$\\epamtestqueue";

        static void Main(string[] args)
        {
            using var loggerFactory = LoggerFactory.Create(builder =>
            {
                builder
                    .AddFilter("Default", LogLevel.Information)
                    .AddConsole();
            });
            _logger = loggerFactory.CreateLogger<Program>();

            _logger.LogInformation("Central service");
            
            Directory.CreateDirectory(_path);

            var factory = new ConnectionFactory() { HostName = "localhost" };
            using (var connection = factory.CreateConnection())
            using (var channel = connection.CreateModel())
            {
                channel.QueueDeclare(_queueName, true, false, false, null);

                channel.BasicQos(0, 0, false);

                var consumer = new EventingBasicConsumer(channel);
                consumer.Received += GetFile;
                channel.BasicConsume(_queueName, false, consumer);

                Console.WriteLine("Press Enter to exit.");
                Console.ReadLine();
            }
        }

        private static void GetFile(object sender, BasicDeliverEventArgs ea)
        {
            var isChunk = (bool)ea.BasicProperties.Headers["isChunk"];
            var name = Encoding.UTF8.GetString((byte[])ea.BasicProperties.Headers["name"]);

            if (!isChunk)
                GetEntireFile(sender, ea, name);         
            else
                GetFileInChunks(sender, ea, name);
            
        }

        private static void GetEntireFile(object sender, BasicDeliverEventArgs ea, string name)
        {
            _logger.LogInformation($"{name} received");
            if (File.Exists(Path.Combine(_path, name)))
            {
                File.Delete(Path.Combine(_path, name));
            }

            var body = ea.Body.ToArray();

            File.WriteAllBytes(Path.Combine(_path, name), body);
            _logger.LogInformation($"{name} saved to {_path}");

            ((EventingBasicConsumer)sender)?.Model.BasicAck(ea.DeliveryTag, false);
            _logger.LogInformation($"{name} saved to {Path.Combine(_path, name)}");
        }

        private static void GetFileInChunks(object sender, BasicDeliverEventArgs ea, string name)
        {
            var chunkPosition = (int)ea.BasicProperties.Headers["chunkPosition"];
            var chunksCount = (long)ea.BasicProperties.Headers["chunksCount"];
            _logger.LogInformation($"{name} received chunk {chunkPosition + 1} out of {chunksCount}");

            if (chunkPosition == 0 && File.Exists(Path.Combine(_path, name)))
            {
                File.Delete(Path.Combine(_path, name));
            }

            var body = ea.Body.ToArray();

            using (var stream = new FileStream(Path.Combine(_path, name), FileMode.Append))
            {
                stream.Write(body);
            }

            if (chunkPosition + 1 == chunksCount)
            {
                ((EventingBasicConsumer)sender)?.Model.BasicAck(ea.DeliveryTag, true);
                _logger.LogInformation($"{name} saved to {Path.Combine(_path, name)}");
            }
        }
    }
}
