import pika, time
from os import error, getenv
from dotenv import load_dotenv
import json

#Funções independentes para enviar uma mensagem a fila, consumir mensagens da fila ou apenas se conectar na fila
class TratamentoFila:
    load_dotenv()
    def __init__(self) -> None:
        pass

    #Função que é chamada para tratar mensagens recebidas na fila
    def callback(ch, method, properties, body):
        job_recebido = json.loads(body)
        jobTratado = TratamentoFila.processamento_job(job_recebido)
        if(jobTratado):
            print('Job recebido e tratado:::')
            ch.basic_ack(delivery_tag = method.delivery_tag)
            time.sleep(180)
        else:
            print('recebida tarefa invalida para este consumidor:::')   
            ch.basic_ack(delivery_tag = method.delivery_tag)
            print('Ainda consumindo mensagem:::')

    #Função que efetivamente trata a mensagem, dizendo se deve ser aceita ou não
    #Poderia estar tudo no callback, mas para melhor entendimento foi separada
    #Se o job recebido for valido para esse sistema, retorna True para a msg ser ack
    #Se não, retorna False para que a msg seja encaminhada para outra fila ou excluida
    def processamento_job(job_recebido):
        try:
            print(job_recebido)
        except KeyError:
            print('Há algum erro no job que o impede de ser conferido::')
            return False
        except error:
            print(error)
            print('Houve um erro inesperado no tratamento da carga::')
            return True

    #Função que escuta a fila
    #Assim que chamada, cria um canal de comunicação com o servidor rabbit e passa a escutar mensagens postadas na fila esperada
    #Caso haja erro de conexão (cai internet, servidor rabbit cai, etc.), a função espera um tempo em segundos e se chama novamente,
    #criando uma nova conexão
    def consumirFila(queue_carga):
        url = getenv(('RABBIT_CONNECTION_STRING'))
        params = pika.URLParameters(url)
        connection = pika.BlockingConnection(params)
        channel = connection.channel()
        channel.queue_declare(queue=queue_carga, durable = True)
        channel.basic_consume(queue_carga,
            TratamentoFila.callback,
            auto_ack=False)
        try:
            print('Conectado ao servidor RabbitMQ:::::')
            channel.start_consuming()  
        except KeyboardInterrupt:
            print('Interrupção pelo operador. Encerrando o serviço::::::')
            channel.stop_consuming()
            connection.close()
        except pika.exceptions.StreamLostError:
            print ('Erro na conexão com a fila')
            time.sleep(180)
            TratamentoFila.consumirFila()

    #Função que envia mensagens para uma fila
    #Ela própria cria uma conexão com o servidor rabbit, encerrando a conexão assim que enviada a mensagem
    def envioJobFila(fila,queue_carga,job):
        try:
            url = getenv(('RABBIT_CONNECTION_STRING'))
            params = pika.URLParameters(url)
            connection = pika.BlockingConnection(params)
            channel = connection.channel()
            channel.queue_declare(queue=queue_carga, durable = True)
            channel.basic_consume(queue_carga,
                TratamentoFila.callback,
                auto_ack=False)
            channel.basic_publish(exchange='', routing_key=fila, body=json.dumps(job))
        except error:
            print(error)