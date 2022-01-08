using RabbitMQ.Client;
using System.Text;
using System.Text.Json;
using TaskCenter.Dto.Virtual;

class Program
{
    static void Main(string[] args)
    {
        int counter = 10;    

        for (int i = 0;i < counter; i++)
        {

            List<TaskDetails> list = new();
            list.Add(new TaskDetails { Company = "AOC", Process = "MARKETPLACE.PRODUCT", Script = string.Empty });
            list.Add(new TaskDetails { Company = "APPLE", Process = "MARKETPLACE.PRODUCT", Script = string.Empty });
            list.Add(new TaskDetails { Company = "SAMSUNG", Process = "MARKETPLACE.PRODUCT", Script = string.Empty });
            list.Add(new TaskDetails { Company = "LG", Process = "MARKETPLACE.PRODUCT", Script = string.Empty });
            list.Add(new TaskDetails { Company = "CISCO", Process = "MARKETPLACE.PRODUCT", Script = string.Empty });
            list.Add(new TaskDetails { Company = "LACOSTE", Process = "MARKETPLACE.PRODUCT", Script = string.Empty });
            list.Add(new TaskDetails { Company = "GAP", Process = "MARKETPLACE.PRODUCT", Script = string.Empty });
            list.Add(new TaskDetails { Company = "AOC", Process = "MARKETPLACE.STOCK", Script = string.Empty });
            list.Add(new TaskDetails { Company = "APPLE", Process = "MARKETPLACE.STOCK", Script = string.Empty });
            list.Add(new TaskDetails { Company = "SAMSUNG", Process = "MARKETPLACE.STOCK", Script = string.Empty });
            list.Add(new TaskDetails { Company = "LG", Process = "MARKETPLACE.STOCK", Script = string.Empty });
            list.Add(new TaskDetails { Company = "CISCO", Process = "MARKETPLACE.STOCK", Script = string.Empty });
            list.Add(new TaskDetails { Company = "LACOSTE", Process = "MARKETPLACE.STOCK", Script = string.Empty });
            list.Add(new TaskDetails { Company = "GAP", Process = "MARKETPLACE.STOCK", Script = string.Empty });

            ConnectionFactory factory = new()
            {
                Uri = new Uri("amqp://usertaskcenter:passtaskcenter@127.0.0.1:5672/taskCenter_virtual_host"),
                NetworkRecoveryInterval = TimeSpan.FromSeconds(10),
                AutomaticRecoveryEnabled = true
            };

            using var connection = factory.CreateConnection();

            using var model = connection.CreateModel();

            //confirmar o envio da mensagem.
            model.ConfirmSelect();

            //declara a exchange 
            model.ExchangeDeclare(exchange: "Task_Client", type: "topic", durable: true, autoDelete: false, arguments: null);

            foreach (var company in list)
            {
                //declara a fila

                model.QueueDeclare(queue: $"Task_Client_{company.Company}", durable: true, exclusive: false, autoDelete: false, arguments: null);
                // efetua o bind entre a fila e a exchange
                model.QueueBind(queue: $"Task_Client_{company.Company}", exchange: "Task_Client", routingKey: $"{company.Process}.{company.Company}", arguments: null);

                string message = JsonSerializer.Serialize(company);

                var body = Encoding.UTF8.GetBytes(message);

                // precisa criar as propriedades pois precisa usar o DeliveryMode = 2 Persiste, 1 Apenas memoria(nessa caso se o rabbit cair as mensagems somem)
                var props = model.CreateBasicProperties();
                props.Headers = new Dictionary<string, Object> { { "content-type", "application/json" } };
                // Persiste
                props.DeliveryMode = 2;

                model.BasicPublish(exchange: "Task_Client", routingKey: $"{company.Process}.{company.Company}", basicProperties: props, body: body);
            }

            Thread.Sleep(300000);
        }
        
    }
}
