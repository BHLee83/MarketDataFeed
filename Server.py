import socket
import threading
import json

# 서버 정보 설정
HOST = '0.0.0.0'  # 모든 IP에서 접속 허용
PORT = 5000

# 각 클라이언트별 데이터를 저장할 딕셔너리
client_data = {}

# 클라이언트별 수신 데이터를 통합하는 함수
def handle_client(conn, addr):
    ip_address = addr[0]
    buffer = ""  # 데이터 버퍼

    while True:
        try:
            data = conn.recv(1024).decode()  # 데이터를 수신하여 디코딩
            if not data:
                break
            
            buffer += data
            while "\n" in buffer:
                message, buffer = buffer.split("\n", 1)  # 구분자를 기준으로 분리
                try:
                    received_data = json.loads(message)
                    client_data[ip_address] = received_data
                    print(f"{ip_address}에서 수신한 데이터:", received_data)
                    print("통합 데이터:", client_data)
                except json.JSONDecodeError:
                    print(f"{ip_address}에서 수신한 데이터 디코딩 실패: {data}")
            
        except ConnectionResetError:
            print(f"{ip_address} 연결 끊김.")
            break
    conn.close()
    # # 클라이언트 연결이 종료되면 데이터에서 제거
    # del client_data[ip_address]
    # print(f"{ip_address} 연결 종료. 통합 데이터에서 제거됨.")

# 서버 소켓 설정
server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
server_socket.bind((HOST, PORT))
server_socket.listen()

print("서버가 시작되었습니다.")

try:
    while True:
        conn, addr = server_socket.accept()  # 클라이언트 연결 대기
        thread = threading.Thread(target=handle_client, args=(conn, addr))
        thread.start()
except KeyboardInterrupt:
    print("\n프로그램을 종료합니다...")
except Exception as e:
    print(f"오류 발생: {str(e)}")
finally:
    server_socket.close()   # 서버 소켓 닫기
    print("서버가 종료되었습니다.")
