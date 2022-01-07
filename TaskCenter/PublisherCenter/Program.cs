using RabbitMQ.Client;
using System.Text;
using System.Text.Json;
using TaskCenter.Dto.Virtual;

class Program
{
    static void Main(string[] args)
    {
        ConnectionFactory factory = new ()
        {
            Uri = new Uri("amqp://usertaskcenter:passtaskcenter@127.0.0.1:5672/taskCenter_virtual_host"),
            NetworkRecoveryInterval = TimeSpan.FromSeconds(10),
            AutomaticRecoveryEnabled = true
        };

        using var connection = factory.CreateConnection();  

        using var model  = connection.CreateModel();

        //confirmar o envio da mensagem.
        model.ConfirmSelect();

        //declara a exchange 
        model.ExchangeDeclare(exchange:"Task_Client", type:"topic", durable: true,autoDelete:false, arguments: null);
        //declara a fila
        model.QueueDeclare(queue:"Task_Client_Company",durable:true,exclusive:false, autoDelete:false,arguments:null);
        // efetua o bind entre a fila e a exchange
        model.QueueBind(queue: "Task_Client_Company", exchange: "Task_Client",routingKey:"process.company",arguments:null);

        TaskDetails taskDetails = new() { Company = "Company",  Process = "B2B.PRODUCT", Script = "select  top 10 produto, desc_produto from produtos" };

        string message = JsonSerializer.Serialize(taskDetails); 
        var body = Encoding.UTF8.GetBytes(message);

        // precisa criar as propriedades pois precisa usar o DeliveryMode = 2 Persiste, 1 Apenas memoria(nessa caso se o rabbit cair as mensagems somem)
        var props = model.CreateBasicProperties();
        props.Headers = new Dictionary<string, Object> { { "content-type", "application/json" } };
        // Persiste
        props.DeliveryMode = 2;

        model.BasicPublish(exchange: "Task_Client", routingKey: "process.company",basicProperties:props, body:body);
    }
}
