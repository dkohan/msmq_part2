using System;
using System.Collections.Generic;
using System.IO;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using RabbitMQ.Client;

namespace Client
{
    public class Program
    {        
        private static ILogger _logger;
        private static string _path = "C:\\MsmqInputFolder";
        private static string _queueName = ".\\Private$\\epamtestqueue";
        private static int _maxSize = 4096;

        static void Main(string[] args)
        {

            using var loggerFactory = LoggerFactory.Create(builder =>
            {
                builder
                    .AddFilter("Default", LogLevel.Information)
                    .AddConsole();
            });

            _logger = loggerFactory.CreateLogger<Program>();
            _logger.LogInformation("Input service.");
            _logger.LogInformation($"Watching directory: {_path}");

            Directory.CreateDirectory(_path);
            using var watcher = new FileSystemWatcher(_path);

            watcher.Created += OnCreated;

            watcher.Filter = "*.txt";
            watcher.EnableRaisingEvents = true;

            Console.WriteLine("Press enter to exit.");
            Console.ReadLine();
        }

        private static void OnCreated(object sender, FileSystemEventArgs e)
        {
            _logger.LogInformation($"{e.Name} created.");
            SendFile(e.FullPath);
        }

        private static void SendFile(string path)
        {            
            var factory = new ConnectionFactory() { HostName = "localhost" };
            using (var connection = factory.CreateConnection())
            using (var channel = connection.CreateModel())
            {
                channel.QueueDeclare(_queueName, true, false, false, null);

                if (new FileInfo(path).Length <= _maxSize)
                {
                    SendEntireFile(path, channel, _queueName);
                }
                else
                {
                    SendFileInChunks(path, channel, _maxSize, _queueName);
                }
                _logger.LogInformation($"{Path.GetFileName(path)} sent.");

                File.Delete(path);
            }
        }

        private static IBasicProperties GetBasicProperties(string path, IModel channel)
        {
            var properties = channel.CreateBasicProperties();
            properties.Persistent = true;
            properties.Headers = new Dictionary<string, object>()
            {
                {"name", Path.GetFileName(path)}
            };
            return properties;
        }

        private static void SendFileInChunks(string path, IModel channel, int maxMessageSize, string queueName)
        {
            byte[] buffer = new byte[maxMessageSize];
            int index = 0;

            using (Stream fs = File.OpenRead(path))
            {
                var chunksCount = (fs.Length + maxMessageSize - 1) / maxMessageSize;
                int read;
                while ((read = fs.Read(buffer, 0, buffer.Length)) > 0)
                {
                    var chunkProperties = GetBasicProperties(path, channel);
                    chunkProperties.Headers.Add("isChunk", true);
                    chunkProperties.Headers.Add("chunkPosition", index);
                    chunkProperties.Headers.Add("chunksCount", chunksCount);

                    _logger.LogInformation($"Sending chunk {index + 1} out of {chunksCount}");
                    channel.BasicPublish("", queueName, true, chunkProperties, new ReadOnlyMemory<byte>(buffer, 0, read));
                    index++;
                }
            }
        }

        private static void SendEntireFile(string path, IModel channel, string queueName)
        {
            _logger.LogInformation("Sending entire file.");
            var basicProperties = GetBasicProperties(path, channel);
            basicProperties.Headers.Add("isChunk", false);
            var body = File.ReadAllBytes(path);
            channel.BasicPublish("", queueName, basicProperties, body);
        }
    }
}
