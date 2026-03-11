from prefect import flow, task
import socket
import multiprocessing
import time
import os


@task(name="CPU 부하 생성 태스크")
def cpu_stress_task(core_id: int, duration_sec: int):
    pid = os.getpid()
    print(f"🔥 [Core {core_id}] {duration_sec}초 부하 시작 (PID: {pid})")

    start_time = time.time()
    # 실제 CPU 점유를 위한 연산 루프
    while time.time() - start_time < duration_sec:
        _ = 100 * 100

    print(f"✅ [Core {core_id}] 테스트 완료")
    return f"Core {core_id} 완료"

# stress_test.py 수정 부분
@flow(name="Worker Stress Test", log_prints=True)
def stress_test_flow(stress_seconds: int = 60, split_count: int = None):
    # 만약 split_count가 문자열로 들어오거나 이상한 값이면 기본값 처리
    try:
        if split_count is not None:
            cpu_count = int(split_count)
        else:
            cpu_count = multiprocessing.cpu_count()
    except (ValueError, TypeError):
        cpu_count = multiprocessing.cpu_count()
        print(f"⚠️ 잘못된 split_count 값이 입력되어 기본값({cpu_count})을 사용합니다.")


if __name__ == "__main__":
    stress_test_flow(stress_seconds=30)