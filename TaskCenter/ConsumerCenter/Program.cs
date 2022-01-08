using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System.Text;
using System.Text.Json;
using TaskCenter.Dto.Virtual;

class Program
{
    static void Main(string[] args)
    {

        ConnectionFactory factory = new()
        {
            Uri = new Uri("amqp://usertaskcenter:passtaskcenter@127.0.0.1:5672/taskCenter_virtual_host"),
            NetworkRecoveryInterval = TimeSpan.FromSeconds(10),
            AutomaticRecoveryEnabled = true
        };

        using var connection = factory.CreateConnection();

        using var model = connection.CreateModel();
        //declara a exchange 
        model.ExchangeDeclare(exchange: "Task_Center", type: "topic", durable: true, autoDelete: false, arguments: null);

        //declara a fila
        model.QueueDeclare(queue: $"Task_Center", durable: true, exclusive: false, autoDelete: false, arguments: null);
        // efetua o bind entre a fila e a exchange
        model.QueueBind(queue: $"Task_Center", exchange: "Task_Center", routingKey: $"*", arguments: null);

        //Configuração do prefetch . Prefetch é a quantidade de mensagem que fica em memoria aguardando o processamento.
        // exemplo se você tiver 1.000 msgs e o prefetch tiver 100 ele irá deixar 900 no disco e mandará 100 para a memoria do consumidor.
        // se tiver 2 consumidares fica 800 no disco e sem na memoria de cada consumidor. numero ideal desse cara é 20x a quantidade de mensagens que um consumidor pode consumir
        model.BasicQos(0, 1000, false);

        var consumer = new EventingBasicConsumer(model);
        consumer.Received += (innerModel, ea) =>
        {
            var body = ea.Body.ToArray();
            string message = Encoding.UTF8.GetString(body);

            string routKey = ea.RoutingKey;
            if (routKey.Contains("MARKETPLACE.STOCK"))
                ServiceStock(model, ea,message);

            if (routKey.Contains("MARKETPLACE.PRODUCT"))
                ServiceProduct(model, ea,message);

            model.BasicAck(ea.DeliveryTag, false);
        };
        model.BasicConsume(queue: $"Task_Center", autoAck: false, consumer: consumer);
        Console.ReadLine();
    }

    static void ServiceStock(IModel model,BasicDeliverEventArgs ea, string message)
    {
        Stocks stock = new();
        try
        {
            #pragma warning disable CS8600 // Converting null literal or possible null value to non-nullable type.
            stock = JsonSerializer.Deserialize<Stocks>(message);
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
            if(stock != null)
                Console.WriteLine($"Company:{stock.Company} - Product:{stock.Product} - Quantity:{stock.Stock}");
        }
        catch (Exception)
        {
            //fala que a mensagem nao foi processada e coloca na fila novamente 
            model.BasicNack(ea.DeliveryTag, false, true);
            throw;
        }
    }
    static void ServiceProduct(IModel model, BasicDeliverEventArgs ea,string message)
    {
        Products product = new();
        try
        {
            #pragma warning disable CS8600 // Converting null literal or possible null value to non-nullable type.
            product = JsonSerializer.Deserialize<Products>(message);
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
            if (product != null)
                Console.WriteLine($"Company:{product.Company} - Product:{product.Product} - Description:{product.Description}");
        }
        catch (Exception)
        {
            //fala que a mensagem nao foi processada e coloca na fila novamente 
            model.BasicNack(ea.DeliveryTag, false, true);
            throw;
        }
    }
}
