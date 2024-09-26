from flask import Flask, request, jsonify
import uuid
import json
from confluent_kafka import Producer
from os import environ 

app = Flask(__name__)

conf = {
    'bootstrap.servers': environ.get('KAFKA_SERVER')  
}
producer = Producer(**conf)

def delivery_report(err, msg):
    """ Callback que se ejecuta cuando se entrega el mensaje o si hay un error """
    if err is not None:
        print(f'Error al entregar el mensaje: {err}')
    else:
        print(f'Mensaje entregado a {msg.topic()} [{msg.partition()}]')

@app.route('/votar', methods=['POST'])
def votar():
    try:
        data = request.get_json()
        voto = data.get('voto')

        if not voto:
            return jsonify({'error': 'El voto es requerido'}), 400

        voto_id = str(uuid.uuid4())
        usuario_id = str(uuid.uuid4())  

        mensaje = {
            'id': voto_id,
            'user_id': usuario_id,
            'vote': voto
        }

        mensaje_json = json.dumps(mensaje)

        producer.produce('voting', mensaje_json.encode('utf-8'), callback=delivery_report)

        producer.flush()

        return jsonify({'status': 'Voto enviado', 'voto_id': voto_id}), 200

    except Exception as e:
        return jsonify({'error': str(e)}), 500

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5000)
