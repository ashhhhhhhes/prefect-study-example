from prefect import flow, task
import socket
import multiprocessing
import time
import os


@task(name="CPU 부하 생성")
def cpu_stress_task(duration_sec: int):
    print(f"🔥 {duration_sec}초 동안 CPU 부하 테스트를 시작합니다... (PID: {os.getpid()})")
    start_time = time.time()
    # CPU 1개를 100% 점유하는 루프
    while time.time() - start_time < duration_sec:
        _ = 100 * 100
    return "부하 테스트 완료"


@flow(name="Worker Stress Test", log_prints=True)
def stress_test_flow(stress_seconds: int = 60):
    host_name = socket.gethostname()
    cpu_count = multiprocessing.cpu_count()

    print(f"--- 테스트 시작 ---")
    print(f"📍 실행 인스턴스: {host_name}")
    print(f"🧵 사용 가능한 CPU 코어 수: {cpu_count}")

    # 부하 작업 실행
    result = cpu_stress_task(stress_seconds)

    print(f"✅ {result} 및 Flow 종료")


if __name__ == "__main__":
    stress_test_flow()