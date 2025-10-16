import Pyro5.api
import Pyro5.errors
import threading
import time
from enum import Enum
import socket
import sys
from name_server import NameServerManager 

PEER_NAMES = ["PeerA", "PeerB", "PeerC", "PeerD"]

# Configurações
COMMTIMEOUT = 10.0      
TIMEOUT_HEARTBEAT = 7   # Tempo limite para considerar um peer falho
HEARTBEAT_INTERVAL = 2  # Intervalo de envio do heartbeat

class PeerState(Enum):
    RELEASED = 1
    WANTED = 2    
    HELD = 3      

def _send_reply_thread(proxy_peer_name, self_peer_name, self_peer, permission):
    """Envia o REPLY de forma assíncrona, criando o proxy na thread."""
    try:
        proxy = self_peer._get_peer_proxy_by_name(proxy_peer_name) 
        if proxy:
            proxy.receive_reply(self_peer_name, permission)
    except (Pyro5.errors.CommunicationError, Exception) as e:
        pass

def _send_request_thread(requester_name, requester_timestamp, proxy_peer_name, requesting_peer):
    """Envia o REQUEST de forma assíncrona, criando o proxy na thread."""
    
    proxy = requesting_peer._get_peer_proxy_by_name(proxy_peer_name)
    if not proxy:
        return requesting_peer._remove_failed_peer(proxy_peer_name) 
    
    try:
        proxy.handle_request(requester_name, requester_timestamp)
        
    except Pyro5.errors.TimeoutError:
        requesting_peer._remove_failed_peer(proxy_peer_name)
    except (Pyro5.errors.CommunicationError, Pyro5.errors.NamingError, Exception) as e:
        requesting_peer._remove_failed_peer(proxy_peer_name)

def _send_heartbeat_thread(proxy_peer_name, self_peer_name, self_peer):
    """Cria o proxy e envia o heartbeat oneway."""
    try:
        proxy = self_peer._get_peer_proxy_by_name(proxy_peer_name)
        if proxy:
            proxy.heartbeat(self_peer_name)
    except Exception:
        pass

def _wait_and_enter_thread(self_peer, duration):
    """Executa a lógica de espera por permissões e entrada/saída da SC."""
    
    # Espera por permissões
    #start_time = time.time()
    while True:
        if self_peer.stop_event.is_set(): return
        with self_peer.lock:
            active_count = len(self_peer.active_peers) + 1 
            if self_peer.reply_count >= active_count:
                break
        time.sleep(0.1)

    # Verifica o resultado da espera
    with self_peer.lock:
        active_count = len(self_peer.active_peers) + 1
        
        if self_peer.reply_count >= active_count and self_peer.state == PeerState.WANTED:
            self_peer._enter_critical_section(duration)
        else:
            if self_peer.state == PeerState.WANTED:
                print(f"{self_peer.name}: FALHA: Não recebeu todas as permissões a tempo ({self_peer.reply_count}/{active_count}). Liberando requisição...")
                self_peer.state = PeerState.RELEASED
                self_peer.request_timestamp = float('inf')
                self_peer.reply_count = 0

def _input_thread(peer):
    """Lógica de leitura de teclado, executada em thread secundária (não bloqueia o Daemon)."""
    
    while not peer.stop_event.is_set():
        try:
            print(f"\n{peer.name}:\n1: Requisitar recurso (10s max)\n2: Liberar Recurso\n3: Listar peers ativos\n4: Sair")
            mode = input("> ")

            if mode == '1':
                peer.request_access(duration=10) 
            elif mode == '2':
                peer.release_access()
            elif mode == '3':
                peer.print_active_peers()
            elif mode == '4':
                break
            else:
                print("Comando inválido.")

        except (EOFError, KeyboardInterrupt):
            break
        except Exception as e:
            print(f"Erro no input: {e}")
            break
    
    peer.stop()

def setup_peer(peer_names, name):
    """Cria, registra e inicia o Peer no Name Server."""
    ns_proxy = NameServerManager.get_ns_proxy()
    if not ns_proxy:
        raise Exception("Não foi possível iniciar ou localizar o Name Server.")
        
    peer = Peer(name, peer_names)
    try:
        ns_proxy.register(name, peer.get_uri())
        print(f"Peer {name} registrado no Name Server.")
    except Exception as e:
        print(f"Erro ao registrar {name}: {e}")
        raise
            
    peer.start()
    return peer

@Pyro5.api.expose
class Peer:
    def __init__(self, name, all_peer_names):
        self.name = name
        self.all_peer_names = sorted([p for p in all_peer_names if p != name])
        
        self.releasing_access = False 
        self.state = PeerState.RELEASED 
        self.clock = 0
        self.mode = 0                 
        self.request_timestamp = float('inf') 
        self.deferred_requests = []         
        self.reply_count = 0                
        self.lock = threading.RLock()       
        
        self.active_peers = set(self.all_peer_names) 
        self.last_contact = {p: time.time() for p in self.all_peer_names} 
        
        self.host = socket.gethostname()
        self.daemon = Pyro5.api.Daemon(host=self.host) 
        self.uri = self.daemon.register(self, self.name)
        
        self.stop_event = threading.Event()

    @Pyro5.api.oneway
    def heartbeat(self, peer_name):
        """Recebe Heartbeat e atualiza o tempo de vida."""

        self.last_contact[peer_name] = time.time()
        if peer_name not in self.active_peers:
            self.active_peers.add(peer_name)
            print(f"\n[{self.name}] Peer {peer_name} re-detectado como ativo via Heartbeat.")

    def _heartbeat_sender(self):
        """Envia Heartbeat periodicamente para todos os peers."""
        while not self.stop_event.is_set():
            time.sleep(HEARTBEAT_INTERVAL)
            
            for peer_name in self.all_peer_names: 
                threading.Thread(
                    target=_send_heartbeat_thread,
                    args=(peer_name, self.name, self),
                    daemon=True
                ).start()

    def _failure_detector(self):
        """Verifica se algum peer ativo falhou baseado no timeout do Heartbeat."""
        while not self.stop_event.is_set():
            time.sleep(HEARTBEAT_INTERVAL * 2) 
            
            with self.lock:
                peers_to_check = list(self.active_peers)
            
            for peer_name in peers_to_check:
                if time.time() - self.last_contact.get(peer_name, 0) > TIMEOUT_HEARTBEAT:
                    self._remove_failed_peer(peer_name)

    def release_access(self):
        """Método chamado pelo usuário (comando 2) para liberar a SC manualmente."""
        print("ola")
        if self.state == PeerState.HELD:
            self.releasing_access = True
            print(f"[{self.name}] Sinal de liberação manual enviado.")
        else:
            print(f"\n[{self.name}] Não está na SC. Liberação ignorada.")
    
    def get_uri(self):
        return self.uri

    def start(self):
        """Inicia as threads de monitoramento (Heartbeat/Detector)."""
        print(f"{self.name}: Iniciando ({self.uri})...")
        
        # Threads de monitoramento e Heartbeat
        self.heartbeat_sender_thread = threading.Thread(target=self._heartbeat_sender, daemon=True)
        self.heartbeat_sender_thread.start()
        
        self.failure_detector_thread = threading.Thread(target=self._failure_detector, daemon=True)
        self.failure_detector_thread.start()
        
    def run_daemon_loop(self):
        """Assume a thread principal e executa o loop de escuta do daemon."""
        try:
            self.daemon.requestLoop() 
        except Exception as e:
            print(f"Daemon falhou: {e}")
        finally:
            self.stop_event.set()

    def stop(self):
        self.stop_event.set()
        self.daemon.shutdown()
        
    def print_active_peers(self):
        """Lista os peers atualmente considerados ativos."""
        print("\n--- Peers Ativos ---")
        with self.lock:
            for name in sorted(list(self.active_peers)):
                print(f"- {name}")
            if not self.active_peers:
                print("Nenhum outro peer ativo detectado.")
        print("--------------------")

    def _get_peer_proxy_by_name(self, peer_name):
        """Obtém o proxy de um peer pelo Name Server (chamado por uma thread)."""
        try:
            ns = Pyro5.api.locate_ns(host=self.host, port=9090)
            uri = ns.lookup(peer_name)
            proxy = Pyro5.api.Proxy(uri)
            return proxy
        except (Pyro5.errors.NamingError, Exception) as e:
            return None

    def _update_clock(self, received_clock):
        """Atualiza o relógio lógico de Lamport."""
        with self.lock:
            self.clock = max(self.clock, received_clock) + 1

    def _remove_failed_peer(self, peer_name):
        """Remove um peer da lista de ativos devido a falha/timeout (thread-safe)."""
        if self.lock.acquire(timeout=0.1): 
            try:
                if peer_name in self.active_peers:
                    print(f"\n{self.name}: Peer {peer_name} REMOVIDO por falha/timeout.") 
                    self.active_peers.remove(peer_name)
                    
                    if self.state == PeerState.WANTED:
                        self.reply_count += 1 
            finally:
                self.lock.release()

    def request_access(self, duration=30):
        """Inicia a thread de espera e a thread de comunicação."""
        
        with self.lock:
            if self.state != PeerState.RELEASED:
                return False

            self._update_clock(0)
            self.request_timestamp = self.clock
            self.state = PeerState.WANTED
            self.reply_count = 1 
        
        print(f"\n{self.name}: REQUISITANDO SC (T={self.request_timestamp}, C={self.clock})...")

        peers_to_wait = []
        with self.lock:
            peers_to_wait = list(self.active_peers)

        #if not peers_to_wait:
            #threading.Thread(target=_wait_and_enter_thread, args=(self, duration), daemon=True).start()
           # return True

        threading.Thread(target=_wait_and_enter_thread, args=(self, duration), daemon=True).start()

        for peer_name in peers_to_wait:
            threading.Thread(
                target=_send_request_thread,
                args=(self.name, self.request_timestamp, peer_name, self),
                daemon=True
            ).start()
        
        return True

    @Pyro5.api.expose
    def handle_request(self, requester_name, requester_timestamp):
        """Processo Pj recebe REQUEST de Pi (unicast)."""
        self._update_clock(requester_timestamp)
        
        reply_immediately = False
        
        with self.lock:
            my_priority = (self.request_timestamp, self.name)
            
            if self.state == PeerState.HELD:
                defer_request = True
            
            elif self.state == PeerState.WANTED:
                requester_priority = (requester_timestamp, requester_name)
                
                if requester_priority < my_priority:
                    reply_immediately = True
                else:
                    defer_request = True
            
            else: # RELEASED
                reply_immediately = True

            if not reply_immediately:
                self._send_reply(requester_name, False)
                self.deferred_requests.append((requester_timestamp, requester_name))
                self.deferred_requests.sort() 
                print(f"{self.name}: REQ de {requester_name} adiado. Fila: {len(self.deferred_requests)}")
            
        if reply_immediately:
            self._send_reply(requester_name, True)
        
        return True 

    def _send_reply(self, peer_name, permission):
        """Envia um REPLY para o peer, em uma thread separada."""
        
        if peer_name not in self.active_peers:
            return

        threading.Thread(
            target=_send_reply_thread, 
            args=(peer_name, self.name, self, permission), 
            daemon=True
        ).start()
    
    @Pyro5.api.oneway
    def receive_reply(self, sender_name, permission):
        """Recebe REPLY de um peer."""
        with self.lock:
            if self.state == PeerState.WANTED and permission == True:
                self.reply_count += 1
                print(f"{self.name}: Recebeu REPLY de {sender_name}. Contagem: {self.reply_count}")

    def _enter_critical_section(self, duration):
        """Entra na SC e espera pelo timeout ou comando manual."""
        
        with self.lock:
            self.state = PeerState.HELD
            self.reply_count = 0
            self.releasing_access = False 
        
        print(f"\n***** {self.name}: ACESSO CONCEDIDO (ENTROU NA ZONA CRÍTICA) | T={self.request_timestamp}, D={duration}s *****")
        
        try:
            start_time = time.time()
            
            while (self.releasing_access == False) and (time.time() - start_time < duration):
                if self.stop_event.is_set(): break
                time.sleep(0.1) 
                
            if self.releasing_access == False:
                 print(f"[{self.name}] Tempo de acesso ({duration}s) EXPIRADO. Liberando recurso.")
                 self.releasing_access = True 
                 
        finally:
            self._exit_critical_section()

    def _exit_critical_section(self):
        """Sai da SC e envia REPLYs adiados."""
        
        self.releasing_access = False 
        
        print(f"{self.name}: <- SAIU da SC. Enviando REPLYs adiados")
        
        deferred_list = []
        with self.lock:
            self.state = PeerState.RELEASED
            self.request_timestamp = float('inf')
            deferred_list = self.deferred_requests[:]
            self.deferred_requests.clear()
        
        for _, requester_name in deferred_list: 
            self._send_reply(requester_name, True)


def main():
    
    name = input("Qual o nome do Peer (PeerA, PeerB, PeerC, PeerD)? ")
    if name not in PEER_NAMES:
        print("Nome inválido. Saindo.")
        return

    peer = None
    try:
        peer = setup_peer(PEER_NAMES, name)
        print(f"\nPeer {name} pronto para interagir. Status: RELEASED")

        input_thread = threading.Thread(target=_input_thread, args=(peer,), daemon=True)
        input_thread.start()
        
        peer.run_daemon_loop() 

    except Exception as e:
        print(f"Erro fatal: {e}")
    finally:
        if peer:
            peer.stop()

if __name__ == "__main__":
    main()