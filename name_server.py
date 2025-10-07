import Pyro5.api
import Pyro5.errors
import Pyro5.nameserver
import threading
import time
import socket
import sys

NS_HOST = socket.gethostname()
NS_PORT = 9090                
NAMESERVER_LITERAL_NAME = "Pyro.NameServer" 

Pyro5.config.SERVERTYPE = "thread" # Força o uso de threads
Pyro5.config.THREADPOOL_SIZE = 30  # Pool grande para Daemons
Pyro5.config.COMMTIMEOUT = 10.0    # Timeout RPC global (10.0s)
Pyro5.config.NS_BCHOST = '0.0.0.0' # Desativa o Broadcast para estabilidade
Pyro5.config.NS_BCPORT = 0          # Desativa o Broadcast

class NameServerManager:
    """Gerencia a inicialização e obtenção do Name Server do Pyro5."""
    
    _ns_daemon = None
    _ns_thread = None

    @staticmethod
    def _start_ns_thread():
        """Inicia o Name Server e seu Daemon de forma explícita na thread."""
        print(f"Tentando iniciar o Name Server em {NS_HOST}:{NS_PORT}...")
        
        try:
            NameServer = Pyro5.nameserver.NameServer()
            daemon = Pyro5.api.Daemon(host=NS_HOST, port=NS_PORT)

            uri = daemon.register(NameServer, NAMESERVER_LITERAL_NAME)
            
            NameServerManager._ns_daemon = daemon
            
            print(f"Name Server iniciado com sucesso. URI: {uri}")

            daemon.requestLoop() 
            
        except Pyro5.errors.CommunicationError as e:
            print(f"Erro ao iniciar o Name Server (porta em uso ou outro erro): {e}")
        except Exception as e:
             print(f"Erro inesperado ao iniciar o Name Server: {e}")
             sys.exit(1)


    @staticmethod
    def get_ns_proxy():
        """(0,3) Tenta obter uma referência do Name Server."""
        try:
            ns = Pyro5.api.locate_ns(host=NS_HOST, port=NS_PORT) 
            print("Referência do Name Server obtida (já estava em execução).")
            return ns
        except (Pyro5.errors.NamingError, Pyro5.errors.CommunicationError):
            if NameServerManager._ns_thread is None or not NameServerManager._ns_thread.is_alive():
                NameServerManager._ns_thread = threading.Thread(
                    target=NameServerManager._start_ns_thread, daemon=True
                )
                NameServerManager._ns_thread.start()
                
                time.sleep(1.5) 
            
            try:
                ns = Pyro5.api.locate_ns(host=NS_HOST, port=NS_PORT)
                return ns
            except (Pyro5.errors.NamingError, Pyro5.errors.CommunicationError) as e:
                print(f"ERRO: Name Server não pôde ser encontrado ou iniciado. Failed to locate the nameserver - {e}")
                return None

    @staticmethod
    def shutdown_ns():
        """Desliga o Daemon do Name Server se ele foi iniciado por este processo."""
        if NameServerManager._ns_daemon:
            print("\nDesligando Name Server Daemon...")
            NameServerManager._ns_daemon.shutdown()
            print("Name Server desligado.")