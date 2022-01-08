using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System.Text;
using System.Text.Json;
using TaskCenter.Dto.Virtual;

class Program
{
    static void Main(string[] args)
    {
        string company = "AOC";

        ConnectionFactory factory = new()
        {
            Uri = new Uri("amqp://usertaskcenter:passtaskcenter@127.0.0.1:5672/taskCenter_virtual_host"),
            NetworkRecoveryInterval = TimeSpan.FromSeconds(10),
            AutomaticRecoveryEnabled = true
        };

        using var connection = factory.CreateConnection();

        using var model = connection.CreateModel();
        //declara a exchange 
        model.ExchangeDeclare(exchange: "Task_Client", type: "topic", durable: true, autoDelete: false, arguments: null);

        //declara a fila
        model.QueueDeclare(queue: $"Task_Client_{company}", durable: true, exclusive: false, autoDelete: false, arguments: null);
        // efetua o bind entre a fila e a exchange
        model.QueueBind(queue: $"Task_Client_{company}", exchange: "Task_Client", routingKey: $"*.*.{company}", arguments: null);

        //Configuração do prefetch . Prefetch é a quantidade de mensagem que fica em memoria aguardando o processamento.
        // exemplo se você tiver 1.000 msgs e o prefetch tiver 100 ele irá deixar 900 no disco e mandará 100 para a memoria do consumidor.
        // se tiver 2 consumidares fica 800 no disco e sem na memoria de cada consumidor. numero ideal desse cara é 20x a quantidade de mensagens que um consumidor pode consumir
        model.BasicQos(0, 1000, false);

        var consumer = new EventingBasicConsumer(model);
        consumer.Received += (innerModel, ea) =>
         {
             var body = ea.Body.ToArray();
             string message = Encoding.UTF8.GetString(body);
             TaskDetails task = new ();
             try
             {
                 #pragma warning disable CS8600 // Converting null literal or possible null value to non-nullable type.
                 task = JsonSerializer.Deserialize<TaskDetails>(message);
                 #pragma warning restore CS8600 // Converting null literal or possible null value to non-nullable type.
             }
             catch (Exception)
             {
                 //rejeita a mensagem e nao coloca na fila de novo, uma vez que deu erro de deserialização
                 model.BasicReject(ea.DeliveryTag, false);
                 throw;
             }

             try
             {
                 if (task != null)
                 {

                     var list = new List<dynamic>();
                     if (task.Process.ToUpper().Trim().Equals("MARKETPLACE.STOCK"))
                     {
                         list.Add(new { Product = "AAAAA", Stock = 2 });
                         list.Add(new { Product = "BBBBB", Stock = 4 });
                         list.Add(new { Product = "CCCCC", Stock = 5 });
                         list.Add(new { Product = "DDDDD", Stock = 6 });
                     }

                     if (task.Process.ToUpper().Trim().Equals("MARKETPLACE.PRODUCT"))
                     {
                         list.Add(new { Product = "AAAAA", Description = "Product AAAAA"});
                         list.Add(new { Product = "BBBBB", Description = "Product BBBBB" });
                         list.Add(new { Product = "CCCCC", Description = "Product CCCCC" });
                         list.Add(new { Product = "DDDDD", Description = "Product DDDDD" });
                     }
                     model.BasicAck(ea.DeliveryTag, false);
                 }
             }
             catch (Exception)
             {
                 //fala que a mensagem nao foi processada e coloca na fila novamente 
                 model.BasicNack(ea.DeliveryTag, false, true);
                 throw;
             }
         };

        model.BasicConsume(queue: $"Task_Client_{company}", autoAck: false, consumer: consumer);
        Console.ReadLine();
    }
}
