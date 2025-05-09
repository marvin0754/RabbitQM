# main.py
import subprocess

# Ejecutar servidor_rpc.py
subprocess.Popen(["python", "servidor_rpc.py"])

# Ejecutar app Flask (amqpstorm_rpc.py, por ejemplo)
subprocess.Popen(["python", "amqpstorm_rpc.py"])
