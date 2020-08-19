using System;
using System.Net.Http.Headers;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka;
using Serilog;

namespace Producer
{
    class Program
    {
        static async Task Main(string[] args)
        {
            var logger = new LoggerConfiguration()
                .WriteTo.Console()
                .CreateLogger();
            logger.Information("Testando o envio de mensagens com Kafka");

            if (args.Length < 3)
            {
                logger.Error(
                    "Informe ao menos 3 parâmetros: " +
                    "no primeiro o IP/porta para testes com o Kafka, " +
                    "no segundo o Topic que receberá a mensagem, " +
                    "já no terceito em diante as mensagens a serem " +
                    "enviadas a um Topic no Kafka...");
                return;
            }

            string bootstrapServers = args[0];
            string nomeTopic = args[1];

            logger.Information($"BootstrapServers = {bootstrapServers}");
            logger.Information($"Topic = {nomeTopic}");

            try
            {
                var config = new ProducerConfig
                {
                    BootstrapServers = bootstrapServers,
                    Partitioner = Partitioner.ConsistentRandom
                };

                using (var producer = new ProducerBuilder<Null, string>(config).Build())
                {

                    while (true)
                    {
                        for (int i = 2; i < args.Length; i++)
                        {
                            var result = await producer.ProduceAsync(
                                nomeTopic,
                                new Message<Null, string>
                                { Value = args[i] + ": " + DateTime.Now.ToString("HH:mm:ss") });

                            logger.Information(
                                $"Mensagem: {args[i]} | " +
                                $"Status: { result.Status.ToString()}");
                        }
                        logger.Information("Concluído o envio de mensagens");
                        Thread.Sleep(5000);
                    }
                }

            }
            catch (Exception ex)
            {
                logger.Error($"Exceção: {ex.GetType().FullName} | " +
                             $"Mensagem: {ex.Message}");
            }
        }
     
    }
}