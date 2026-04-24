'''
- 로그 생성 => 파일 기록
- json 형태, 텍스트(한줄에 로그기록 작성) 형태
'''

# 1. 모듈 가져오기
import json
import time
import datetime
import os

# 2. 로그가 저장되는 디렉토리 지정/생성
log_dir = './sensor_logs'
if not os.path.exists(log_dir):
    os.makedirs(log_dir)


# 3. 로그 발생 및 저장
def generate_logs():
    # 로그 샘플
    # 시간을 제외한 모든 값 고정
    data = {
        "timestamp" : datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "sensor_id" : "AI-FACTORY-001",
        "temperature" : 87.5,
        "humidity"  : 42.4,
        "status"    : "RUNNING" 
    }

    # json 형태로 파일 기록(한줄에 로그 1개씩) -> dict 객체 직렬화 처리
    # 파일명 ./sensor_log_/sensor_json.log
    # 한줄에 JSON 객체 1개씩 문자열로 기록(JSON Lines : JSONL)
    # a(append) -> 파일에 새로운 내용을 추가
    with open(f"{log_dir}/sensor_json.log","a",encoding="utf-8") as f:
        f.write(json.dumps(data) + "\n")

    # text 형태로 파일 기록(한줄에 로그 1개씩) -> f-string 구성
    # 파일명 ./sensor_log/sensor_text.log
    text = f'[{data["timestamp"]}] ID={data["sensor_id"]} |  TEMP:{data["temperature"]} |   HUMI:{data["humidity"]} |   STAT:{data["status"]}'
    with open(f"{log_dir}/sensor_text.log","a",encoding="utf-8") as f:
        f.write(json.dumps(text) + "\n")

    print(f"로그 발생 완료 { data['timestamp'] }")
# 4. 로그 발생기 가동
def main():
    try:
        while True:
            generate_logs()
            time.sleep(2)
    except Exception as e:
        print('종료 완료')
    pass

# 5. 프로그램 시작
if __name__  == '__main__':
    print('로그발생 시작. 종료 ctrl+c')
    main()

