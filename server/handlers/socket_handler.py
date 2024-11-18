from datetime import datetime
import socket
import threading
import capnp


class SocketHandler:
    def __init__(self, buffer_size=65536):
        self.buffer_size = buffer_size
        self.client_locks = {}
        self.connection_status = {}
        self.reconnect_attempts = {}  # 재연결 시도 횟수 추적
        self.max_reconnect_attempts = 3
        self.reconnect_delay = 5  # 초단위
        
    def setup_client(self, client_socket: socket.socket, client_id: str):
        """새로운 클라이언트 설정"""
        self.client_locks[client_id] = threading.Lock()
        self.connection_status[client_id] = {
            'connected_at': datetime.now(),
            'last_active': datetime.now(),
            'status': 'connected',
            'socket': client_socket
        }
        
    def receive_data(self, client_socket: socket.socket) -> bytes:
        """청크 단위로 데이터 수신"""
        # 메시지 크기 수신
        size_data = client_socket.recv(4)
        if not size_data:
            return b''
            
        msg_size = int.from_bytes(size_data, byteorder='big')
        
        # 전체 데이터 수신
        data = b''
        remaining = msg_size
        while remaining > 0:
            chunk = client_socket.recv(min(remaining, self.buffer_size))
            if not chunk:
                break
            data += chunk
            remaining -= len(chunk)
            
        return data
        
    def validate_market_data(self, market_data) -> bool:
        """데이터 검증"""
        try:
            # 필드가 존재하는지 확인
            required_fields = ['source', 'timestamp', 'dataType', 'content']
            for field in required_fields:
                if not hasattr(market_data, field):
                    return False

            # content 필드가 capnp 리스트인지 확인
            if not isinstance(market_data.content, capnp.lib.capnp._DynamicListReader):
                return False

            return True
        except Exception as e:
            print(f"데이터 검증 오류: {e}")
            return False
        
    def handle_connection_failure(self, client_id: str):
        """클라이언트 연결 실패 처리"""
        if client_id not in self.reconnect_attempts:
            self.reconnect_attempts[client_id] = 0
            
        if self.reconnect_attempts[client_id] < self.max_reconnect_attempts:
            self.reconnect_attempts[client_id] += 1
            self.connection_status[client_id]['status'] = 'reconnecting'
            print(f"클라이언트 {client_id} 재연결 시도 {self.reconnect_attempts[client_id]}/{self.max_reconnect_attempts}")
            return True
        else:
            self.connection_status[client_id]['status'] = 'disconnected'
            print(f"클라이언트 {client_id} 연결 복구 실패")
            return False
            
    def update_client_status(self, client_id: str, status: str):
        """클라이언트 상태 업데이트"""
        if client_id in self.connection_status:
            self.connection_status[client_id].update({
                'status': status,
                'last_active': datetime.now()
            })