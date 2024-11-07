import win32com.client
import pythoncom
import os
import time
import socket
import json


# 전역 변수로 Excel 관련 객체 선언
excel_app = None
workbook = None
sheet = None
previous_data = {}

# 서버 정보 설정
SERVER_HOST = '211.232.151.210'  # 서버 IP 주소
SERVER_PORT = 5000  # 서버 포트

def initialize_excel():
    """Excel 초기화 및 설정"""
    global excel_app, workbook, sheet
    
    try:
        print("Excel 초기화 시작...")
        # COM 초기화
        pythoncom.CoInitialize()

        # Excel 인스턴스 생성
        excel_app = win32com.client.DispatchEx("Excel.Application")
        excel_app.Visible = True
        
        # Infomax 추가기능 로드
        addin_path = r"C:\Infomax\bin\excel\infomaxexcel.xlam"
        addin_name = "infomaxexcel.xlam"
        
        # 추가기능 로드 시도
        for addon in excel_app.AddIns:
            if addon.Name.lower() == addin_name.lower():
                addon.Installed = True
                break
                
        excel_app.Workbooks.Open(addin_path)
        time.sleep(1)  # 로드 대기
        
        # 워크북 열기
        file_path = r"D:\Public\Infomax_API\AllList_RT.xlsx"
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"File not found: {file_path}")
            
        workbook = excel_app.Workbooks.Open(file_path)
        time.sleep(5)  # 워크북 열기 대기
        
        sheet = workbook.Sheets(1)
        print("Excel 초기화 완료!")
        return True
        
    except Exception as e:
        print(f"Excel 초기화 중 오류 발생: {str(e)}")
        cleanup_excel()
        return False
    
def cleanup_excel():
    """Excel 리소스 정리"""
    global excel_app, workbook, sheet
    
    try:
        if workbook:
            workbook.Save()
            workbook.Close(SaveChanges=True)
        if excel_app:
            excel_app.Quit()
    except Exception as e:
        print(f"cleanup 중 오류 발생: {str(e)}")
    finally:
        excel_app = None
        workbook = None
        sheet = None
        pythoncom.CoUninitialize()
    
def get_excel_data(client_socket):
    """Excel 데이터 읽기"""
    global previous_data, sheet, workbook
    
    try:
        last_row = sheet.Cells(sheet.Rows.Count, 1).End(-4162).Row
        current_data = sheet.Range(f"A2:D{last_row}").Value

        if not current_data:  # 데이터가 없는 경우
            return []

        # 변경된 데이터 추출
        updated_data = []
        for idx, row in enumerate(current_data, start=2):
            product_type, item_name, item_type, current_price = row
            row_data = (product_type, item_name, item_type, current_price)

            # 이전 데이터와 비교하여 변경 사항이 있는 경우에만 저장
            if previous_data.get(idx) != row_data:
                updated_data.append({
                    "row": idx,
                    "product_type": product_type,
                    "item_name": item_name,
                    "item_type": item_type,
                    "current_price": current_price
                })
                previous_data[idx] = row_data  # 이전 데이터 업데이트

        # 업데이트된 데이터가 있는 경우에만 전송
        if updated_data:
            return json.dumps(updated_data) + "\n"  # 구분자 추가
            # client_socket.sendall(data.encode())  # 서버로 데이터 전송
            # print("서버로 데이터 전송:", updated_data)
        return None
    
    except Exception as e:
        print(f"데이터 읽기 중 오류 발생: {str(e)}")
        return []
 
def connect_to_server():
    """서버에 연결 시도. 연결될 때까지 재시도"""
    while True:
        try:
            client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            client_socket.connect((SERVER_HOST, SERVER_PORT))
            print("서버에 연결되었습니다.")
            return client_socket
        except ConnectionRefusedError:
            print("서버에 연결할 수 없습니다. 5초 후에 재시도합니다...")
            time.sleep(5)  # 5초 후 재시도

if __name__ == '__main__':
    try:
        # Excel 초기화 먼저 수행
        if initialize_excel():
            # 서버 연결 시도 (연결될 때까지 대기)
            client_socket = connect_to_server()
            print("서버에 연결되었습니다.")
            
            # 데이터 지속 전송
            while True:
                data = get_excel_data(client_socket)
                if data:
                    client_socket.sendall(data.encode())  # 서버로 데이터 전송
                    print("서버로 데이터 전송:", data)
                # time.sleep(1)  # 1초마다 데이터 갱신
        else:
            print("Excel 초기화 실패로 프로그램을 종료합니다.")
    except KeyboardInterrupt:
        print("\n프로그램을 종료합니다...")
    finally:
        cleanup_excel()
        if 'client_socket' in locals():
            client_socket.close()  # 소켓 연결 종료