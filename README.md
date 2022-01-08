# TaskCenter - RabbitMQ
Centralizador de tarefas.

Projeto de estudos de mensageria RabbitMQ.
Publisher gerenciador principal que envia mensagens para works clientes.
Workes efetuam a tarefa e faz um publisher com mensagem para o rabbit novamente com o retorno dos dados
Um Worker principal processa a mensagem nova que veio do works clientes
